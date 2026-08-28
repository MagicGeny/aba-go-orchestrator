package repository

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func quotaDateParam(quotaDate time.Time) string {
	return quotaDate.Format("2006-01-02")
}

func (r *PostgresRepository) CountMappedPhones(ctx context.Context, tenantID uuid.UUID, messengerType string, phones []string) (int, error) {
	if len(phones) == 0 {
		return 0, nil
	}
	if messengerType == "" {
		messengerType = domain.DefaultMessengerType
	}
	var n int
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT phone_normalized)
		FROM chat_phone_mappings
		WHERE tenant_id = $1
		  AND messenger_type = $2
		  AND phone_normalized = ANY($3)`,
		tenantID, messengerType, phones).Scan(&n)
	return n, err
}

func (r *PostgresRepository) GetTenantsWithProcessingCampaigns(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT tenant_id
		FROM campaigns
		WHERE status = 'processing' AND deleted = FALSE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *PostgresRepository) GetOrCreateTenantDailyQuota(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, coldMin, coldMax int) (*domain.TenantDailyQuota, error) {
	if coldMax < coldMin {
		coldMax = coldMin
	}
	coldLimit := coldMin
	if coldMax > coldMin {
		coldLimit = coldMin + rand.Intn(coldMax-coldMin+1)
	}
	date := quotaDateParam(quotaDate)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO tenant_daily_quotas (tenant_id, quota_date, cold_limit)
		VALUES ($1, $2::date, $3)
		ON CONFLICT (tenant_id, quota_date) DO NOTHING`,
		tenantID, date, coldLimit)
	if err != nil {
		return nil, fmt.Errorf("create tenant daily quota: %w", err)
	}

	var q domain.TenantDailyQuota
	err = r.pool.QueryRow(ctx, `
		SELECT tenant_id, quota_date, cold_limit, cold_used, warm_used, last_cold_publish_at, created_at, updated_at
		FROM tenant_daily_quotas
		WHERE tenant_id = $1 AND quota_date = $2::date`,
		tenantID, date).Scan(
		&q.TenantID, &q.QuotaDate, &q.ColdLimit, &q.ColdUsed, &q.WarmUsed, &q.LastColdPublishAt, &q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("load tenant daily quota: %w", err)
	}
	return &q, nil
}

func (r *PostgresRepository) GetNextPendingWarmTarget(ctx context.Context, tenantID uuid.UUID) (*domain.PendingTargetForDosing, error) {
	return r.getNextPendingTarget(ctx, tenantID, true)
}

func (r *PostgresRepository) GetNextPendingColdTarget(ctx context.Context, tenantID uuid.UUID) (*domain.PendingTargetForDosing, error) {
	return r.getNextPendingTarget(ctx, tenantID, false)
}

func (r *PostgresRepository) getNextPendingTarget(ctx context.Context, tenantID uuid.UUID, warm bool) (*domain.PendingTargetForDosing, error) {
	// If warm=true, we return any pending target regardless of whether it has
	// a chat_phone_mapping. The LEFT JOIN LATERAL below populates ChatID when
	// a mapping exists; scheduleTarget then sets use_chat_id based on that.
	// This prevents the pipeline from stalling when targets have no mapping
	// (e.g. because the recipient doesn't exist in MAX or it's a new number).
	//
	// If warm=false, we still return any pending target too — the caller
	// (doseTenant) applies cold-specific gates (work window, cold limit,
	// cold interval) before calling this path.
	mappingClause := ""
	if !warm {
		// Cold path: only targets that have NO chat_phone_mapping at all.
		// These are genuinely new contacts that should be subject to the
		// cold anti‑ban gates (work window, cold limit, cold interval).
		mappingClause = `
		AND NOT EXISTS (
			SELECT 1 FROM chat_phone_mappings m
			WHERE m.tenant_id = c.tenant_id
			  AND m.phone_normalized = ct.phone_normalized
			  AND m.messenger_type = COALESCE(ct.messenger_type, 'MAX')
		)`
	}

	query := `
		SELECT
			ct.id,
			ct.campaign_id,
			c.tenant_id,
			ct.client_name,
			ct.phone_normalized,
			c.message_template,
			COALESCE(m.chat_id, ''),
			COALESCE(ct.messenger_type, 'MAX'),
			c.attachment_url,
			c.attachment_name
		FROM campaign_targets ct
		JOIN campaigns c ON c.id = ct.campaign_id
		LEFT JOIN LATERAL (
			SELECT chat_id
			FROM chat_phone_mappings
			WHERE tenant_id = c.tenant_id
			  AND phone_normalized = ct.phone_normalized
			  AND messenger_type = COALESCE(ct.messenger_type, 'MAX')
			LIMIT 1
		) m ON TRUE
		WHERE c.tenant_id = $1
		  AND c.status = 'processing'
		  AND c.deleted = FALSE
		  AND ct.status = 'pending'
		  AND NOT EXISTS (
			SELECT 1 FROM tenant_blocked_recipients b
			WHERE b.tenant_id = c.tenant_id AND b.phone_normalized = ct.phone_normalized
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM outbox_messages o
			WHERE o.payload->>'task_id' = ct.id::text
		  )
		` + mappingClause + `
		ORDER BY ct.created_at ASC
		FOR UPDATE OF ct SKIP LOCKED
		LIMIT 1`

	var t domain.PendingTargetForDosing
	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&t.TargetID, &t.CampaignID, &t.TenantID, &t.ClientName, &t.PhoneNormalized,
		&t.MessageTemplate, &t.ChatID, &t.MessengerType, &t.AttachmentURL, &t.AttachmentName,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.IsWarm = warm
	return &t, nil
}

func (r *PostgresRepository) CreateDosedOutboxMessage(ctx context.Context, tenantID uuid.UUID, eventType string, payload []byte, publishAt time.Time) error {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO outbox_messages (id, event_type, payload, status, publish_at, tenant_id)
		VALUES ($1, $2, $3, 'pending', $4, $5)`,
		id, eventType, payload, publishAt, tenantID)
	return err
}

func (r *PostgresRepository) IncrementColdUsed(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tenant_daily_quotas
		SET cold_used = cold_used + 1, updated_at = NOW()
		WHERE tenant_id = $1 AND quota_date = $2::date`,
		tenantID, quotaDateParam(quotaDate))
	return err
}

func (r *PostgresRepository) IncrementWarmUsed(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tenant_daily_quotas
		SET warm_used = warm_used + 1, updated_at = NOW()
		WHERE tenant_id = $1 AND quota_date = $2::date`,
		tenantID, quotaDateParam(quotaDate))
	return err
}

func (r *PostgresRepository) UpdateLastColdPublishAt(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, at time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tenant_daily_quotas
		SET last_cold_publish_at = $3, updated_at = NOW()
		WHERE tenant_id = $1 AND quota_date = $2::date`,
		tenantID, quotaDateParam(quotaDate), at)
	return err
}
