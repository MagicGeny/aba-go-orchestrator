package worker

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/google/uuid"
)

// CampaignDoser schedules outbox messages per tenant respecting cold/warm daily limits.
type CampaignDoser struct {
	repo domain.CampaignRepository
	cfg  config.Config
}

func NewCampaignDoser(repo domain.CampaignRepository, cfg config.Config) *CampaignDoser {
	return &CampaignDoser{repo: repo, cfg: cfg}
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
			return d.scheduleTarget(ctx, target, quotaDate, now, false, 0)
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
	return d.scheduleTarget(ctx, target, quotaDate, now, true, interval)
}

func CapitalizeText(s string) string {
	return cases.Title(language.Russian).String(strings.ToLower(strings.TrimSpace(s)))
}

func (d *CampaignDoser) scheduleTarget(ctx context.Context, target *domain.PendingTargetForDosing, quotaDate, now time.Time, isCold bool, interval time.Duration) error {
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
		TaskID:         target.TargetID.String(),
		CampaignID:     target.CampaignID.String(),
		TenantID:       target.TenantID.String(),
		Messenger:      strings.ToLower(messengerType),
		MessengerType:  messengerType,
		Phone:          target.PhoneNormalized,
		MessageText:    messageText,
		UseChatID:      useChatID,
		ChatID:         target.ChatID,
		ContactType:    contactType,
		AttachmentURL:  target.AttachmentURL,
		AttachmentName: target.AttachmentName,
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
		return d.repo.CreateDosedOutboxMessage(ctx, target.TenantID, eventType, payload, at)
	}

	// For warm messages, apply the interval delay
	warmPublishAt := at.Add(interval)
	if err := d.repo.CreateDosedOutboxMessage(ctx, target.TenantID, eventType, payload, warmPublishAt); err != nil {
		return err
	}
	return d.repo.IncrementWarmUsed(ctx, target.TenantID, quotaDate)
}
