package repository

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/logging"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// retryTransitionFields builds the correlation fields for the
// TARGET_STATUS_UPDATE_* diagnostics emitted by the retry pipeline. These
// events are logging only: they never influence the returned values.
func retryTransitionFields(targetID, accountID uuid.UUID, oldStatus, newStatus domain.TaskStatus, attemptCount, maxAttempts int, errorCode string) map[string]any {
	account := ""
	if accountID != uuid.Nil {
		account = accountID.String()
	}
	return logging.Fields(
		"target_id", targetID.String(),
		"task_id", targetID.String(),
		"account_id", account,
		"old_status", string(oldStatus),
		"new_status", string(newStatus),
		"attempt_count", attemptCount,
		"max_attempts", maxAttempts,
		"error_code", errorCode,
	)
}

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
	// Warm = pending/retry_pending target that already has a chat_id mapping (repeat contact).
	// Cold = pending/retry_pending target with no mapping.
	`` // Eligibility is driven by the target's own status/attempt_count, not by outbox_messages.status.
	// A pending target with attempt_count > 0 already has an in-flight outbox and is NOT re-selected
	// until the worker returns a terminal or retryable result.  This prevents the duplicate-dosing
	// race that occurred when only a pending outbox row blocked re-selection (the row becomes
	// 'processed' once RabbitMQ publishes, before the worker returns).
	mappingClause := `
		AND EXISTS (
			SELECT 1 FROM chat_phone_mappings m
			WHERE m.tenant_id = c.tenant_id
			  AND m.phone_normalized = ct.phone_normalized
			  AND m.messenger_type = COALESCE(ct.messenger_type, 'MAX')
			  AND m.chat_id IS NOT NULL AND m.chat_id <> ''
		)`
	if !warm {
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
			c.attachment_name,
			m.tenant_account_id,
			COALESCE(ct.attempt_count, 0)
		FROM campaign_targets ct
		JOIN campaigns c ON c.id = ct.campaign_id
		LEFT JOIN LATERAL (
			SELECT chat_id, tenant_account_id
			FROM chat_phone_mappings
			WHERE tenant_id = c.tenant_id
			  AND phone_normalized = ct.phone_normalized
			  AND messenger_type = COALESCE(ct.messenger_type, 'MAX')
			ORDER BY updated_at DESC
			LIMIT 1
		) m ON TRUE
		WHERE c.tenant_id = $1
		  AND c.status = 'processing'
		  AND c.deleted = FALSE
		  AND (
		        (ct.status = 'pending' AND COALESCE(ct.attempt_count, 0) = 0)
		     OR (ct.status = 'retry_pending' AND (ct.next_attempt_at IS NULL OR ct.next_attempt_at <= NOW()))
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM tenant_blocked_recipients b
			WHERE b.tenant_id = c.tenant_id AND b.phone_normalized = ct.phone_normalized
		  )
		` + mappingClause + `
		ORDER BY ct.created_at ASC
		FOR UPDATE OF ct SKIP LOCKED
		LIMIT 1`

	var t domain.PendingTargetForDosing
	var preferredAccount *uuid.UUID
	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&t.TargetID, &t.CampaignID, &t.TenantID, &t.ClientName, &t.PhoneNormalized,
		&t.MessageTemplate, &t.ChatID, &t.MessengerType, &t.AttachmentURL, &t.AttachmentName,
		&preferredAccount, &t.AttemptCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.IsWarm = warm
	t.PreferredAccount = preferredAccount
	return &t, nil
}

func (r *PostgresRepository) CreateDosedOutboxMessage(ctx context.Context, tenantID uuid.UUID, eventType string, payload []byte, publishAt time.Time) error {
	// Legacy helper kept for compatibility; prefer AssignAccountAndEnqueueTarget.
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	var accountID uuid.UUID
	err = r.pool.QueryRow(ctx, `
		SELECT id FROM tenant_accounts
		WHERE tenant_id = $1
		ORDER BY created_at ASC, id ASC
		LIMIT 1`, tenantID).Scan(&accountID)
	if err != nil {
		return fmt.Errorf("resolve default tenant account: %w", err)
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO outbox_messages (id, event_type, payload, status, publish_at, tenant_id, tenant_account_id)
		VALUES ($1, $2, $3, 'pending', $4, $5, $6)`,
		id, eventType, payload, publishAt, tenantID, accountID)
	return err
}

func (r *PostgresRepository) AssignAccountAndEnqueueTarget(ctx context.Context, targetID, tenantID, accountID uuid.UUID, eventType string, payload []byte, publishAt time.Time) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// Atomically lock the target row and verify it is still eligible for a new attempt.
	var attemptCount int
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(attempt_count, 0)
		FROM campaign_targets
		WHERE id = $1
		  AND (
		        (status = 'pending' AND COALESCE(attempt_count, 0) = 0)
		     OR (status = 'retry_pending' AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()))
		  )
		FOR UPDATE`, targetID).Scan(&attemptCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load target for claim: %w", err)
	}

	usageDate := publishAt.UTC().Format("2006-01-02")
	tag, err := tx.Exec(ctx, `
		INSERT INTO tenant_account_daily_usage (tenant_account_id, usage_date, used_count)
		VALUES ($1, $2::date, 1)
		ON CONFLICT (tenant_account_id, usage_date) DO UPDATE
		SET used_count = tenant_account_daily_usage.used_count + 1,
		    updated_at = NOW()
		WHERE tenant_account_daily_usage.used_count < (
			SELECT daily_limit FROM tenant_accounts WHERE id = $1
		)`, accountID, usageDate)
	if err != nil {
		return false, fmt.Errorf("reserve account daily slot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, fmt.Errorf("account daily limit exhausted: %s", accountID)
	}

	_, err = tx.Exec(ctx, `
		UPDATE campaign_targets
		SET tenant_account_id = $1,
		    tenant_id = $2,
		    status = 'pending',
		    attempt_count = COALESCE(attempt_count, 0) + 1,
		    next_attempt_at = NULL,
		    updated_at = NOW()
		WHERE id = $3`, accountID, tenantID, targetID)
	if err != nil {
		return false, fmt.Errorf("assign account to target: %w", err)
	}

	outboxID, err := uuid.NewV7()
	if err != nil {
		outboxID = uuid.New()
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages (id, event_type, payload, status, publish_at, tenant_id, tenant_account_id)
		VALUES ($1, $2, $3, 'pending', $4, $5, $6)`,
		outboxID, eventType, payload, publishAt, tenantID, accountID)
	if err != nil {
		return false, fmt.Errorf("insert dosed outbox: %w", err)
	}
	return true, tx.Commit(ctx)
}

func (r *PostgresRepository) ListAssignableTenantAccounts(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, now time.Time) ([]*domain.TenantAccount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			ta.id, ta.tenant_id, ta.account_key, ta.phone_number, ta.proxy_url, ta.status,
			ta.daily_limit, ta.cooldown_until, ta.created_at, ta.updated_at,
			COALESCE(u.used_count, 0)
		FROM tenant_accounts ta
		LEFT JOIN tenant_account_daily_usage u
		  ON u.tenant_account_id = ta.id AND u.usage_date = $2::date
		WHERE ta.tenant_id = $1
		  AND ta.status <> 'disabled'
		  AND (
		        ta.status = 'active'
		     OR ta.cooldown_until IS NULL
		     OR ta.cooldown_until <= $3
		  )
		ORDER BY ta.created_at ASC, ta.id ASC`,
		tenantID, quotaDateParam(quotaDate), now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.TenantAccount
	for rows.Next() {
		var a domain.TenantAccount
		if err := rows.Scan(
			&a.ID, &a.TenantID, &a.AccountKey, &a.PhoneNumber, &a.ProxyURL, &a.Status,
			&a.DailyLimit, &a.CooldownUntil, &a.CreatedAt, &a.UpdatedAt, &a.UsedToday,
		); err != nil {
			return nil, err
		}
		if a.Status == domain.AccountStatusCooldown && (a.CooldownUntil == nil || !a.CooldownUntil.After(now)) {
			a.Status = domain.AccountStatusActive
		}
		if a.IsAssignable(now) {
			accounts = append(accounts, &a)
		}
	}
	return accounts, rows.Err()
}

func (r *PostgresRepository) GetTenantAccountByID(ctx context.Context, accountID uuid.UUID) (*domain.TenantAccount, error) {
	var a domain.TenantAccount
	err := r.pool.QueryRow(ctx, `
		SELECT id, tenant_id, account_key, phone_number, proxy_url, status, daily_limit,
		       cooldown_until, created_at, updated_at
		FROM tenant_accounts WHERE id = $1`, accountID).Scan(
		&a.ID, &a.TenantID, &a.AccountKey, &a.PhoneNumber, &a.ProxyURL, &a.Status,
		&a.DailyLimit, &a.CooldownUntil, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *PostgresRepository) GetTenantAccountByKey(ctx context.Context, tenantID uuid.UUID, accountKey string) (*domain.TenantAccount, error) {
	var a domain.TenantAccount
	err := r.pool.QueryRow(ctx, `
		SELECT id, tenant_id, account_key, phone_number, proxy_url, status, daily_limit,
		       cooldown_until, created_at, updated_at
		FROM tenant_accounts WHERE tenant_id = $1 AND account_key = $2`, tenantID, accountKey).Scan(
		&a.ID, &a.TenantID, &a.AccountKey, &a.PhoneNumber, &a.ProxyURL, &a.Status,
		&a.DailyLimit, &a.CooldownUntil, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *PostgresRepository) MarkAccountCooldown(ctx context.Context, accountID uuid.UUID, until time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tenant_accounts
		SET status = 'cooldown', cooldown_until = $2, updated_at = NOW()
		WHERE id = $1 AND status <> 'disabled'`, accountID, until.UTC())
	return err
}

func (r *PostgresRepository) MarkTargetRetryPending(ctx context.Context, targetID uuid.UUID, errorCode, errorMessage string, nextAttemptAt time.Time, maxAttempts int) (bool, error) {
	return r.ApplyRetryableFailure(ctx, targetID, uuid.Nil, errorCode, errorMessage, nextAttemptAt, time.Time{}, maxAttempts)
}

// ApplyRetryableFailure atomically applies account cooldown (when accountID set and
// error is session-health) and moves the target to retry_pending. Idempotent for
// targets already in retry_pending / delivery_unknown / failed.
func (r *PostgresRepository) ApplyRetryableFailure(ctx context.Context, targetID, accountID uuid.UUID, errorCode, errorMessage string, nextAttemptAt, cooldownUntil time.Time, maxAttempts int) (bool, error) {
	if maxAttempts < 1 {
		maxAttempts = 3
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var attemptCount int
	var status domain.TaskStatus
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(attempt_count, 0), status
		FROM campaign_targets WHERE id = $1 FOR UPDATE`, targetID).Scan(&attemptCount, &status)
	if err != nil {
		logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.Fields(
			"target_id", targetID.String(),
			"task_id", targetID.String(),
			"new_status", string(domain.TaskStatusRetryPending),
			"error_code", errorCode,
			"error", err.Error(),
		))
		return false, err
	}

	switch status {
	case domain.TaskStatusRetryPending:
		// Already awaiting retry — treat as success (idempotent).
		logging.Info("TARGET_STATUS_UPDATED", logging.WithFields(
			retryTransitionFields(targetID, accountID, status, status, attemptCount, maxAttempts, errorCode),
			"retried", true,
			"no_change", true,
			"reason", "already_retry_pending",
		))
		return true, tx.Commit(ctx)
	case domain.TaskStatusDeliveryUnknown, domain.TaskStatusFailed, domain.TaskStatusUserNotFoundByPhone,
		domain.TaskStatusSent, domain.TaskStatusDelivered, domain.TaskStatusViewed, domain.TaskStatusReplied:
		// Terminal or success — do not reopen.
		logging.Info("TARGET_STATUS_UPDATED", logging.WithFields(
			retryTransitionFields(targetID, accountID, status, status, attemptCount, maxAttempts, errorCode),
			"retried", false,
			"no_change", true,
			"reason", "already_terminal",
		))
		return false, tx.Commit(ctx)
	}

	fields := retryTransitionFields(targetID, accountID, status, domain.TaskStatusRetryPending, attemptCount, maxAttempts, errorCode)

	if accountID != uuid.Nil && domain.IsSessionHealthError(errorCode) && !cooldownUntil.IsZero() {
		_, err = tx.Exec(ctx, `
			UPDATE tenant_accounts
			SET status = 'cooldown', cooldown_until = $2, updated_at = NOW()
			WHERE id = $1 AND status <> 'disabled'`, accountID, cooldownUntil.UTC())
		if err != nil {
			logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.WithFields(fields,
				"account_cooldown_until", cooldownUntil.UTC().Format(time.RFC3339),
				"error", err.Error()))
			return false, err
		}
		logging.Info("ACCOUNT_COOLDOWN_APPLIED", logging.Fields(
			"account_id", accountID.String(),
			"target_id", targetID.String(),
			"error_code", errorCode,
			"cooldown_until", cooldownUntil.UTC().Format(time.RFC3339),
		))
	}

	if attemptCount >= maxAttempts {
		fields["new_status"] = string(domain.TaskStatusFailed)
		logging.Info("TARGET_STATUS_UPDATE_START", logging.WithFields(fields,
			"reason", "retries_exhausted"))
		_, err = tx.Exec(ctx, `
			UPDATE campaign_targets
			SET status = 'failed',
			    last_error = $2,
			    last_error_code = $3,
			    last_error_message = $2,
			    next_attempt_at = NULL,
			    updated_at = NOW()
			WHERE id = $1`, targetID, errorMessage, errorCode)
		if err != nil {
			logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.WithFields(fields, "error", err.Error()))
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.WithFields(fields, "error", err.Error()))
			return false, err
		}
		logging.Info("TARGET_STATUS_UPDATED", logging.WithFields(fields,
			"retried", false,
			"reason", "retries_exhausted",
		))
		return false, nil
	}

	logging.Info("TARGET_STATUS_UPDATE_START", logging.WithFields(fields,
		"next_attempt_at", nextAttemptAt.UTC().Format(time.RFC3339),
	))
	_, err = tx.Exec(ctx, `
		UPDATE campaign_targets
		SET status = 'retry_pending',
		    last_error = $2,
		    last_error_code = $3,
		    last_error_message = $2,
		    next_attempt_at = $4,
		    updated_at = NOW()
		WHERE id = $1`, targetID, errorMessage, errorCode, nextAttemptAt.UTC())
	if err != nil {
		logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.WithFields(fields, "error", err.Error()))
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.WithFields(fields, "error", err.Error()))
		return false, err
	}
	logging.Info("TARGET_STATUS_UPDATED", logging.WithFields(fields,
		"retried", true,
		"next_attempt_at", nextAttemptAt.UTC().Format(time.RFC3339),
	))
	return true, nil
}

func (r *PostgresRepository) MarkTargetDeliveryUnknown(ctx context.Context, targetID uuid.UUID, errorCode, errorMessage string) error {
	logging.Info("TARGET_STATUS_UPDATE_START", logging.Fields(
		"target_id", targetID.String(),
		"task_id", targetID.String(),
		"new_status", string(domain.TaskStatusDeliveryUnknown),
		"error_code", errorCode,
	))
	tag, err := r.pool.Exec(ctx, `
		UPDATE campaign_targets
		SET status = 'delivery_unknown',
		    last_error = $2,
		    last_error_code = $3,
		    last_error_message = $2,
		    next_attempt_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		  AND status NOT IN ('sent', 'delivered', 'viewed', 'replied', 'delivery_unknown')`,
		targetID, errorMessage, errorCode)
	if err != nil {
		logging.Error("TARGET_STATUS_UPDATE_FAILED", logging.Fields(
			"target_id", targetID.String(),
			"task_id", targetID.String(),
			"new_status", string(domain.TaskStatusDeliveryUnknown),
			"error_code", errorCode,
			"error", err.Error(),
		))
		return err
	}
	logging.Info("TARGET_STATUS_UPDATED", logging.Fields(
		"target_id", targetID.String(),
		"task_id", targetID.String(),
		"new_status", string(domain.TaskStatusDeliveryUnknown),
		"error_code", errorCode,
		// applied=false means the target was already sent/delivered/viewed,
		// i.e. the guard in the SQL intentionally kept it unchanged.
		"applied", tag.RowsAffected() == 1,
	))
	return nil
}

func (r *PostgresRepository) DeferRetryTargetsWithoutAccounts(ctx context.Context, tenantID uuid.UUID, nextAttemptAt time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE campaign_targets ct
		SET next_attempt_at = $2, updated_at = NOW()
		FROM campaigns c
		WHERE ct.campaign_id = c.id
		  AND c.tenant_id = $1
		  AND c.status = 'processing'
		  AND c.deleted = FALSE
		  AND ct.status = 'retry_pending'
		  AND (ct.next_attempt_at IS NULL OR ct.next_attempt_at <= NOW())`,
		tenantID, nextAttemptAt.UTC())
	return err
}

func (r *PostgresRepository) TryReserveColdSlot(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, at time.Time, minInterval time.Duration) (bool, error) {
	secs := int64(minInterval / time.Second)
	if secs < 0 {
		secs = 0
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE tenant_daily_quotas
		SET cold_used = cold_used + 1,
		    last_cold_publish_at = $3,
		    updated_at = NOW()
		WHERE tenant_id = $1
		  AND quota_date = $2::date
		  AND cold_used < cold_limit
		  AND (
		    last_cold_publish_at IS NULL
		    OR last_cold_publish_at + make_interval(secs => $4::int) <= $3
		  )`,
		tenantID, quotaDateParam(quotaDate), at, secs)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r *PostgresRepository) IncrementWarmUsed(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tenant_daily_quotas
		SET warm_used = warm_used + 1, updated_at = NOW()
		WHERE tenant_id = $1 AND quota_date = $2::date`,
		tenantID, quotaDateParam(quotaDate))
	return err
}
