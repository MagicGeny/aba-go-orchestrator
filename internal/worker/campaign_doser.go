package worker

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/logging"
	"github.com/google/uuid"
)

// CampaignDoser schedules outbox messages per tenant respecting cold/warm daily limits
// and distributes send tasks across tenant accounts with simple Round-Robin.
type CampaignDoser struct {
	repo     domain.CampaignRepository
	cfg      config.Config
	rrMu     sync.Mutex
	rrCursor map[uuid.UUID]int
}

func NewCampaignDoser(repo domain.CampaignRepository, cfg config.Config) *CampaignDoser {
	return &CampaignDoser{
		repo:     repo,
		cfg:      cfg,
		rrCursor: make(map[uuid.UUID]int),
	}
}

func (d *CampaignDoser) Run(ctx context.Context) {
	randTime := 10 + rand.IntN(20-10+1)
	ticker := time.NewTicker(time.Duration(randTime) * time.Second)
	defer ticker.Stop()
	log.Println("CampaignDoser: started")

	for {
		select {
		case <-ctx.Done():
			log.Println("CampaignDoser: stopped")
			return
		case <-ticker.C:
			d.doseAllTenants(ctx)
		}
	}
}

func (d *CampaignDoser) doseAllTenants(ctx context.Context) {
	tenants, err := d.repo.GetTenantsWithProcessingCampaigns(ctx)
	if err != nil {
		log.Printf("CampaignDoser: failed to list tenants: %v", err)
		return
	}
	for _, tenantID := range tenants {
		if err := d.doseTenant(ctx, tenantID); err != nil {
			log.Printf("CampaignDoser: tenant %s dosing failed: %v", tenantID, err)
		}
	}
}

func (d *CampaignDoser) doseTenant(ctx context.Context, tenantID uuid.UUID) error {
	now := time.Now().In(d.cfg.Location)
	quotaDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, d.cfg.Location)

	accounts, err := d.repo.ListAssignableTenantAccounts(ctx, tenantID, quotaDate, now)
	if err != nil {
		return err
	}
	if len(accounts) == 0 {
		next := d.cfg.NextDayWorkStart(now)
		if err := d.repo.DeferRetryTargetsWithoutAccounts(ctx, tenantID, next); err != nil {
			log.Printf("CampaignDoser: tenant %s defer retries failed: %v", tenantID, err)
		}
		return nil
	}

	quota, err := d.repo.GetOrCreateTenantDailyQuota(ctx, tenantID, quotaDate, d.cfg.LimitColdMin, d.cfg.LimitColdMax)
	if err != nil {
		return err
	}

	if quota.WarmUsed < d.cfg.LimitWarmDaily {
		target, err := d.repo.GetNextPendingWarmTarget(ctx, tenantID)
		if err != nil {
			return err
		}
		if target != nil {
			return d.scheduleTarget(ctx, target, accounts, quotaDate, now, false, 0)
		}
	}

	if !d.cfg.IsWithinWorkWindow(now) {
		log.Printf("CampaignDoser: tenant %s outside work window %s-%s (%s), skipping cold sends",
			tenantID, d.cfg.WorkWindowStart, d.cfg.WorkWindowEnd, now.Format("15:04:05 MST"))
		return nil
	}
	if quota.ColdUsed >= quota.ColdLimit {
		return nil
	}
	interval := d.cfg.RandomColdInterval()
	if quota.LastColdPublishAt != nil && now.Sub(*quota.LastColdPublishAt) < interval {
		return nil
	}

	target, err := d.repo.GetNextPendingColdTarget(ctx, tenantID)
	if err != nil {
		return err
	}
	if target == nil {
		return nil
	}
	return d.scheduleTarget(ctx, target, accounts, quotaDate, now, true, interval)
}

func CapitalizeText(s string) string {
	return cases.Title(language.Russian).String(strings.ToLower(strings.TrimSpace(s)))
}

func (d *CampaignDoser) pickAccount(tenantID uuid.UUID, accounts []*domain.TenantAccount, preferred *uuid.UUID) *domain.TenantAccount {
	if len(accounts) == 0 {
		return nil
	}
	if preferred != nil {
		for _, a := range accounts {
			if a.ID == *preferred {
				return a
			}
		}
	}

	d.rrMu.Lock()
	defer d.rrMu.Unlock()
	cursor := d.rrCursor[tenantID]
	idx := cursor % len(accounts)
	d.rrCursor[tenantID] = cursor + 1
	return accounts[idx]
}

// doseTrace builds the correlation fields for the dosing diagnostics
// (TARGET_DOSED / TARGET_DOSE_FAILED). The CampaignTarget -> OutboxMessage
// step of the lifecycle is recorded here, with the attempt number that only
// the doser knows.
func doseTrace(target *domain.PendingTargetForDosing, account *domain.TenantAccount, messengerType, contactType, eventType string, useChatID bool, publishAt time.Time) map[string]any {
	return logging.Fields(
		"task_id", target.TargetID.String(),
		"target_id", target.TargetID.String(),
		"campaign_id", target.CampaignID.String(),
		"tenant_id", target.TenantID.String(),
		"tenant_account_id", account.ID.String(),
		"account_key", account.AccountKey,
		"messenger_type", messengerType,
		"contact_type", contactType,
		"phone", logging.MaskPhone(target.PhoneNormalized),
		"use_chat_id", useChatID,
		"chat_id", target.ChatID,
		"outbox_event_type", eventType,
		"attempt", target.AttemptCount,
		"publish_at", publishAt.UTC().Format(time.RFC3339),
	)
}

func (d *CampaignDoser) scheduleTarget(ctx context.Context, target *domain.PendingTargetForDosing, accounts []*domain.TenantAccount, quotaDate, now time.Time, isCold bool, interval time.Duration) error {
	account := d.pickAccount(target.TenantID, accounts, target.PreferredAccount)
	if account == nil {
		next := d.cfg.NextDayWorkStart(now)
		_ = d.repo.DeferRetryTargetsWithoutAccounts(ctx, target.TenantID, next)
		return nil
	}

	messageText := strings.ReplaceAll(target.MessageTemplate, "{user_name}", CapitalizeText(target.ClientName))
	contactType := "warm"
	useChatID := target.IsWarm && target.ChatID != ""
	if isCold {
		contactType = "cold"
		useChatID = false
	}

	messengerType := target.MessengerType
	if messengerType == "" {
		messengerType = domain.DefaultMessengerType
	}

	eventType := domain.OutboxEventSend
	if useChatID {
		eventType = domain.OutboxEventSendExistingChat
	}

	payload, err := json.Marshal(domain.SendTaskPayload{
		TaskID:          target.TargetID.String(),
		CampaignID:      target.CampaignID.String(),
		TenantID:        target.TenantID.String(),
		TenantAccountID: account.ID.String(),
		AccountID:       account.ID.String(), // routing identity = tenant_account_id UUID
		Messenger:       strings.ToLower(messengerType),
		MessengerType:   messengerType,
		Phone:           target.PhoneNormalized,
		MessageText:     messageText,
		UseChatID:       useChatID,
		ChatID:          target.ChatID,
		ContactType:     contactType,
		AttachmentURL:   target.AttachmentURL,
		AttachmentName:  target.AttachmentName,
	})
	if err != nil {
		return err
	}

	at := now.UTC()
	if isCold {
		reserved, err := d.repo.TryReserveColdSlot(ctx, target.TenantID, quotaDate, at, interval)
		if err != nil {
			return err
		}
		if !reserved {
			return nil
		}
		if err := d.repo.AssignAccountAndEnqueueTarget(ctx, target.TargetID, target.TenantID, account.ID, eventType, payload, at); err != nil {
			log.Printf("CampaignDoser: enqueue failed target=%s tenant_account_id=%s key=%s: %v", target.TargetID, account.ID, account.AccountKey, err)
			logging.Error("TARGET_DOSE_FAILED", logging.WithFields(
				doseTrace(target, account, messengerType, contactType, eventType, useChatID, at),
				"error", err.Error(),
			))
			return nil
		}
		log.Printf("CampaignDoser: cold target=%s tenant_account_id=%s key=%s", target.TargetID, account.ID, account.AccountKey)
		logging.Info("TARGET_DOSED", doseTrace(target, account, messengerType, contactType, eventType, useChatID, at))
		return nil
	}

	warmPublishAt := at.Add(interval)
	if err := d.repo.AssignAccountAndEnqueueTarget(ctx, target.TargetID, target.TenantID, account.ID, eventType, payload, warmPublishAt); err != nil {
		log.Printf("CampaignDoser: warm enqueue failed target=%s tenant_account_id=%s key=%s: %v", target.TargetID, account.ID, account.AccountKey, err)
		logging.Error("TARGET_DOSE_FAILED", logging.WithFields(
			doseTrace(target, account, messengerType, contactType, eventType, useChatID, warmPublishAt),
			"error", err.Error(),
		))
		return nil
	}
	log.Printf("CampaignDoser: warm target=%s tenant_account_id=%s key=%s", target.TargetID, account.ID, account.AccountKey)
	logging.Info("TARGET_DOSED", doseTrace(target, account, messengerType, contactType, eventType, useChatID, warmPublishAt))
	return d.repo.IncrementWarmUsed(ctx, target.TenantID, quotaDate)
}
