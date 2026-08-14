package repository

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) CreateCampaign(ctx context.Context, campaign *domain.Campaign, targets []*domain.CampaignTarget, outbox []*domain.OutboxMessage) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert campaign
	_, err = tx.Exec(ctx, `
		INSERT INTO campaigns (id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, total_count, start_immediately, time_to_start)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		campaign.ID, campaign.TenantID, campaign.Name, campaign.MessageTemplate, campaign.Status, campaign.OriginalExcelName, campaign.OriginalExcelPath, campaign.ProcessedExcelPath, campaign.TotalCount, campaign.StartImmediately, campaign.TimeToStart)
	if err != nil {
		return fmt.Errorf("failed to insert campaign: %w", err)
	}

	// Bulk insert targets using CopyFrom
	targetRows := make([][]any, len(targets))
	for i, t := range targets {
		targetRows[i] = []any{t.ID, t.CampaignID, t.ClientName, t.PhoneNormalized, t.ExcelRowIndex, t.Status}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"campaign_targets"},
		[]string{"id", "campaign_id", "client_name", "phone_normalized", "excel_row_index", "status"},
		pgx.CopyFromRows(targetRows),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk insert targets: %w", err)
	}

	// Bulk insert outbox messages
	outboxRows := make([][]any, len(outbox))
	for i, m := range outbox {
		outboxRows[i] = []any{m.ID, m.EventType, m.Payload, m.Status}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"outbox_messages"},
		[]string{"id", "event_type", "payload", "status"},
		pgx.CopyFromRows(outboxRows),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk insert outbox: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) GetCampaign(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	var c domain.Campaign
	err := r.pool.QueryRow(ctx, `
		SELECT id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start, processed_count, total_count, error_count, created_at, updated_at
		FROM campaigns WHERE id = $1`, id).Scan(
		&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *PostgresRepository) ListCampaigns(ctx context.Context, tenantID uuid.UUID) ([]*domain.Campaign, error) {
	log.Printf("ListCampaigns called for tenant: %v", tenantID)
	rows, err := r.pool.Query(ctx, `
		SELECT id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start, processed_count, total_count, error_count, created_at, updated_at
		FROM campaigns WHERE tenant_id = $1 AND deleted = FALSE ORDER BY created_at DESC`, tenantID)
	if err != nil {
		log.Printf("ListCampaigns query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var campaigns []*domain.Campaign
	for rows.Next() {
		var c domain.Campaign
		err := rows.Scan(&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			log.Printf("ListCampaigns scan error: %v", err)
			return nil, err
		}
		log.Printf("ListCampaigns found campaign: %v, status: %v, deleted: %v", c.ID, c.Status, c.Deleted)
		campaigns = append(campaigns, &c)
	}
	log.Printf("ListCampaigns returning %d campaigns", len(campaigns))
	return campaigns, nil
}

func (r *PostgresRepository) UpdateCampaignStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, "UPDATE campaigns SET status = $1, updated_at = $2 WHERE id = $3", status, now, id)
	return err
}

func (r *PostgresRepository) GetCampaignTargets(ctx context.Context, campaignID uuid.UUID) ([]*domain.CampaignTarget, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, campaign_id, client_name, phone_normalized, excel_row_index, status, last_error, sent_at, replied_at, last_reply_text, created_at, updated_at
		FROM campaign_targets WHERE campaign_id = $1 ORDER BY excel_row_index ASC`, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []*domain.CampaignTarget
	for rows.Next() {
		var t domain.CampaignTarget
		err := rows.Scan(&t.ID, &t.CampaignID, &t.ClientName, &t.PhoneNormalized, &t.ExcelRowIndex, &t.Status, &t.LastError, &t.SentAt, &t.RepliedAt, &t.LastReplyText, &t.CreatedAt, &t.UpdatedAt)
		if err != nil {
			return nil, err
		}
		targets = append(targets, &t)
	}
	return targets, nil
}

func (r *PostgresRepository) GetCampaignsByStatus(ctx context.Context, status domain.CampaignStatus) ([]*domain.Campaign, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start, processed_count, total_count, error_count, created_at, updated_at
		FROM campaigns WHERE status = $1 AND deleted = false`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campaigns []*domain.Campaign
	for rows.Next() {
		var c domain.Campaign
		err := rows.Scan(&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return nil, err
		}
		campaigns = append(campaigns, &c)
	}
	return campaigns, nil
}

func (r *PostgresRepository) GetTargetsByStatus(ctx context.Context, campaignID uuid.UUID, status domain.TaskStatus) ([]*domain.CampaignTarget, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, campaign_id, client_name, phone_normalized, excel_row_index, status, last_error, sent_at, replied_at, last_reply_text, created_at, updated_at
		FROM campaign_targets WHERE campaign_id = $1 AND status = $2`, campaignID, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []*domain.CampaignTarget
	for rows.Next() {
		var t domain.CampaignTarget
		err := rows.Scan(&t.ID, &t.CampaignID, &t.ClientName, &t.PhoneNormalized, &t.ExcelRowIndex, &t.Status, &t.LastError, &t.SentAt, &t.RepliedAt, &t.LastReplyText, &t.CreatedAt, &t.UpdatedAt)
		if err != nil {
			return nil, err
		}
		targets = append(targets, &t)
	}
	return targets, nil
}

func (r *PostgresRepository) GetPendingMessages(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, event_type, payload, status, created_at
		FROM outbox_messages 
		WHERE status = 'pending' 
		FOR UPDATE SKIP LOCKED 
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*domain.OutboxMessage
	for rows.Next() {
		var m domain.OutboxMessage
		err := rows.Scan(&m.ID, &m.EventType, &m.Payload, &m.Status, &m.CreatedAt)
		if err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}
	return messages, nil
}

func (r *PostgresRepository) MarkAsProcessed(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, "UPDATE outbox_messages SET status = 'processed', processed_at = $1 WHERE id = $2", now, id)
	return err
}

func (r *PostgresRepository) UpdateTargetStatus(ctx context.Context, targetID uuid.UUID, status domain.TaskStatus, lastError *string, sentAt *time.Time) (*domain.Campaign, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var campaignID uuid.UUID
	var oldStatus domain.TaskStatus

	// Determine sent_at value: use provided if available, otherwise use UTC now for 'delivered' or 'sent'
	var finalSentAt *time.Time
	if sentAt != nil {
		finalSentAt = sentAt
	} else if status == domain.TaskStatusDelivered || status == domain.TaskStatusSent {
		now := time.Now().UTC()
		finalSentAt = &now
	}

	now := time.Now().UTC()
	err = tx.QueryRow(ctx, "UPDATE campaign_targets SET status = $1, last_error = $2, sent_at = COALESCE($3, sent_at), updated_at = $4 WHERE id = $5 RETURNING campaign_id, status",
		status, lastError, finalSentAt, now, targetID).Scan(&campaignID, &oldStatus)
	if err != nil {
		return nil, err
	}

	// Update campaign counters
	var query string
	var args []interface{}
	if status == domain.TaskStatusDelivered {
		query = "UPDATE campaigns SET processed_count = processed_count + 1, updated_at = $1 WHERE id = $2 RETURNING id, tenant_id, name, message_template, status, original_excel_name, processed_count, total_count, error_count, created_at, updated_at, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start"
		args = []interface{}{now, campaignID}
	} else if status == domain.TaskStatusFailed {
		query = "UPDATE campaigns SET error_count = error_count + 1, updated_at = $1 WHERE id = $2 RETURNING id, tenant_id, name, message_template, status, original_excel_name, processed_count, total_count, error_count, created_at, updated_at, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start"
		args = []interface{}{now, campaignID}
	} else {
		// Just return campaign
		query = "SELECT id, tenant_id, name, message_template, status, original_excel_name, processed_count, total_count, error_count, created_at, updated_at, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start FROM campaigns WHERE id = $1"
		args = []interface{}{campaignID}
	}

	var c domain.Campaign
	err = tx.QueryRow(ctx, query, args...).Scan(
		&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart)
	if err != nil {
		return nil, err
	}

	return &c, tx.Commit(ctx)
}

func (r *PostgresRepository) UpdateCampaign(ctx context.Context, campaign *domain.Campaign) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, `
		UPDATE campaigns 
		SET name = $1, message_template = $2, status = $3, original_excel_path = $4, processed_excel_path = $5, deleted = $6, processed_count = $7, total_count = $8, error_count = $9, start_immediately = $10, time_to_start = $11, updated_at = $12
		WHERE id = $13`,
		campaign.Name, campaign.MessageTemplate, campaign.Status, campaign.OriginalExcelPath, campaign.ProcessedExcelPath, campaign.Deleted, campaign.ProcessedCount, campaign.TotalCount, campaign.ErrorCount, campaign.StartImmediately, campaign.TimeToStart, now, campaign.ID)
	return err
}

func (r *PostgresRepository) GetCampaignTargetsWithStatus(ctx context.Context, campaignID uuid.UUID, statuses []domain.TaskStatus) ([]*domain.CampaignTarget, error) {
	placeholders := make([]string, len(statuses))
	args := make([]interface{}, len(statuses)+1)
	args[0] = campaignID
	for i, status := range statuses {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = status
	}

	query := fmt.Sprintf(`
		SELECT id, campaign_id, client_name, phone_normalized, excel_row_index, status, last_error, sent_at, replied_at, last_reply_text, created_at, updated_at
		FROM campaign_targets 
		WHERE campaign_id = $1 AND status IN (%s)`, strings.Join(placeholders, ","))

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []*domain.CampaignTarget
	for rows.Next() {
		var t domain.CampaignTarget
		err := rows.Scan(&t.ID, &t.CampaignID, &t.ClientName, &t.PhoneNormalized, &t.ExcelRowIndex, &t.Status, &t.LastError, &t.SentAt, &t.RepliedAt, &t.LastReplyText, &t.CreatedAt, &t.UpdatedAt)
		if err != nil {
			return nil, err
		}
		targets = append(targets, &t)
	}
	return targets, nil
}

func (r *PostgresRepository) CreateReply(ctx context.Context, reply *domain.CampaignReply) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO campaign_replies (id, campaign_target_id, message_text, received_at)
		VALUES ($1, $2, $3, $4)
	`, reply.ID, reply.CampaignTargetID, reply.MessageText, reply.ReceivedAt)
	return err
}

func (r *PostgresRepository) GetTenantByID(ctx context.Context, tenantID uuid.UUID) (*domain.Tenant, error) {
	var t domain.Tenant
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, type, COALESCE(admin_phone, ''), admin_messenger, keycloak_group_id, created_at, updated_at
		FROM tenants
		WHERE id = $1
	`, tenantID).Scan(
		&t.ID, &t.Name, &t.Type, &t.AdminPhone, &t.AdminMessenger, &t.KeycloakGroupID, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *PostgresRepository) GetCampaignTargetByID(ctx context.Context, targetID uuid.UUID) (*domain.CampaignTarget, error) {
	var t domain.CampaignTarget
	err := r.pool.QueryRow(ctx, `
		SELECT id, campaign_id, client_name, phone_normalized, excel_row_index, status,
			last_error, sent_at, replied_at, last_reply_text, created_at, updated_at
		FROM campaign_targets
		WHERE id = $1
	`, targetID).Scan(
		&t.ID, &t.CampaignID, &t.ClientName, &t.PhoneNormalized, &t.ExcelRowIndex,
		&t.Status, &t.LastError, &t.SentAt, &t.RepliedAt, &t.LastReplyText, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *PostgresRepository) GetActiveCampaignsReadyToStart(ctx context.Context) ([]*domain.Campaign, error) {
	now := time.Now().UTC()
	rows, err := r.pool.Query(ctx, `
		SELECT id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start, processed_count, total_count, error_count, created_at, updated_at
		FROM campaigns WHERE status IN ('draft', 'processing') 
			AND deleted = FALSE
			AND (start_immediately = TRUE OR (time_to_start IS NOT NULL AND time_to_start <= $1))
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campaigns []*domain.Campaign
	for rows.Next() {
		var c domain.Campaign
		err := rows.Scan(&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return nil, err
		}
		campaigns = append(campaigns, &c)
	}
	return campaigns, nil
}

func (r *PostgresRepository) StopCampaign(ctx context.Context, campaignID uuid.UUID) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, `
		UPDATE campaigns 
		SET status = $1, updated_at = $2
		WHERE id = $3
	`, domain.CampaignStatusStopped, now, campaignID)
	return err
}

func (r *PostgresRepository) StartCampaign(ctx context.Context, campaignID uuid.UUID) error {
	// Get the campaign with its targets and message template
	campaign, err := r.GetCampaign(ctx, campaignID)
	if err != nil {
		return fmt.Errorf("failed to get campaign: %w", err)
	}

	targets, err := r.GetCampaignTargets(ctx, campaignID)
	if err != nil {
		return fmt.Errorf("failed to get targets: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update campaign status to processing
	now := time.Now().UTC()
	_, err = tx.Exec(ctx, "UPDATE campaigns SET status = $1, updated_at = $2 WHERE id = $3",
		domain.CampaignStatusProcessing, now, campaignID)
	if err != nil {
		return fmt.Errorf("failed to update campaign status: %w", err)
	}

	// Create outbox messages for all pending targets
	for _, target := range targets {
		messageText := strings.ReplaceAll(campaign.MessageTemplate, "{user_name}", target.ClientName)

		payload := fmt.Sprintf(`{"task_id":"%s", "campaign_id":"%s", "tenant_id":"%s", "messenger":"max", "phone":"%s", "message_text":%q}`,
			target.ID, campaign.ID, campaign.TenantID, target.PhoneNormalized, messageText)

		outboxID, err := uuid.NewV7()
		if err != nil {
			outboxID = uuid.New()
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_messages (id, event_type, payload, status)
			VALUES ($1, $2, $3, $4)`,
			outboxID, "message.send", []byte(payload), "pending")
		if err != nil {
			return fmt.Errorf("failed to insert outbox message: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) GetRepliesByCampaign(ctx context.Context, campaignID uuid.UUID) ([]*domain.CampaignReply, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT r.id, r.campaign_target_id, r.message_text, r.received_at, r.created_at, r.updated_at
		FROM campaign_replies r
		JOIN campaign_targets t ON r.campaign_target_id = t.id
		WHERE t.campaign_id = $1`, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var replies []*domain.CampaignReply
	for rows.Next() {
		var r domain.CampaignReply
		err := rows.Scan(&r.ID, &r.CampaignTargetID, &r.MessageText, &r.ReceivedAt, &r.CreatedAt, &r.UpdatedAt)
		if err != nil {
			return nil, err
		}
		replies = append(replies, &r)
	}
	return replies, nil
}

func (r *PostgresRepository) RegisterReply(ctx context.Context, campaignID uuid.UUID, phone string, text string, repliedAt time.Time) (*domain.Campaign, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Find target by campaign and phone
	var targetID uuid.UUID
	var oldStatus domain.TaskStatus
	now := time.Now().UTC()
	err = tx.QueryRow(ctx, "UPDATE campaign_targets SET status = $1, last_reply_text = $2, replied_at = $3, updated_at = $4 WHERE campaign_id = $5 AND phone_normalized = $6 RETURNING id, status",
		domain.TaskStatusReplied, text, repliedAt, now, campaignID, phone).Scan(&targetID, &oldStatus)
	if err != nil {
		return nil, err
	}

	// Create reply record
	replyID, err := uuid.NewV7()
	if err != nil {
		replyID = uuid.New()
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO campaign_replies (id, campaign_target_id, message_text, received_at)
		VALUES ($1, $2, $3, $4)`,
		replyID, targetID, text, repliedAt)
	if err != nil {
		return nil, err
	}

	// Get updated campaign
	var c domain.Campaign
	err = tx.QueryRow(ctx, `
		SELECT id, tenant_id, name, message_template, status, original_excel_name, original_excel_path, processed_excel_path, deleted, start_immediately, time_to_start, processed_count, total_count, error_count, created_at, updated_at
		FROM campaigns WHERE id = $1`, campaignID).Scan(
		&c.ID, &c.TenantID, &c.Name, &c.MessageTemplate, &c.Status, &c.OriginalExcelName, &c.OriginalExcelPath, &c.ProcessedExcelPath, &c.Deleted, &c.StartImmediately, &c.TimeToStart, &c.ProcessedCount, &c.TotalCount, &c.ErrorCount, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(text) == "@" {
		now := time.Now().UTC()
		_, err = tx.Exec(ctx, `
			INSERT INTO tenant_blocked_recipients (tenant_id, phone_normalized, blocked_at, updated_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, phone_normalized) DO UPDATE
			SET updated_at = EXCLUDED.updated_at
		`, c.TenantID, phone, repliedAt, now)
		if err != nil {
			return nil, err
		}
	}

	// Check if all targets have replied
	var totalTargets int
	var repliedTargets int
	err = tx.QueryRow(ctx, `
		SELECT 
			COUNT(*) as total,
			SUM(CASE WHEN status = 'replied' THEN 1 ELSE 0 END) as replied
		FROM campaign_targets WHERE campaign_id = $1`, campaignID).Scan(&totalTargets, &repliedTargets)
	if err != nil {
		return nil, err
	}

	// If all replied, mark campaign as completed
	if repliedTargets >= totalTargets {
		now := time.Now().UTC()
		_, err = tx.Exec(ctx, `UPDATE campaigns SET status = 'completed', updated_at = $1 WHERE id = $2`, now, campaignID)
		if err != nil {
			return nil, err
		}
		c.Status = domain.CampaignStatusCompleted
	}

	return &c, tx.Commit(ctx)
}

func (r *PostgresRepository) AddBlockedRecipient(ctx context.Context, tenantID uuid.UUID, phoneNormalized string) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, `
		INSERT INTO tenant_blocked_recipients (tenant_id, phone_normalized, blocked_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, phone_normalized) DO UPDATE
		SET updated_at = EXCLUDED.updated_at
	`, tenantID, phoneNormalized, now, now)
	return err
}

func (r *PostgresRepository) ListBlockedRecipients(ctx context.Context) ([]*domain.BlockedRecipient, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT tenant_id, phone_normalized, blocked_at, created_at, updated_at
		FROM tenant_blocked_recipients
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*domain.BlockedRecipient
	for rows.Next() {
		var br domain.BlockedRecipient
		err := rows.Scan(&br.TenantID, &br.PhoneNormalized, &br.BlockedAt, &br.CreatedAt, &br.UpdatedAt)
		if err != nil {
			return nil, err
		}
		items = append(items, &br)
	}
	return items, nil
}

func (r *PostgresRepository) UpsertChatPhoneMapping(ctx context.Context, mapping *domain.ChatPhoneMapping) error {
	now := time.Now().UTC()
	if mapping.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			id = uuid.New()
		}
		mapping.ID = id
	}
	mapping.UpdatedAt = now
	if mapping.CreatedAt.IsZero() {
		mapping.CreatedAt = now
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO chat_phone_mappings (id, chat_id, campaign_id, campaign_target_id, phone_normalized, viewer_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (chat_id) DO UPDATE
		SET campaign_id = EXCLUDED.campaign_id,
			campaign_target_id = EXCLUDED.campaign_target_id,
			phone_normalized = EXCLUDED.phone_normalized,
			viewer_id = EXCLUDED.viewer_id,
			updated_at = EXCLUDED.updated_at
	`, mapping.ID, mapping.ChatID, mapping.CampaignID, mapping.CampaignTargetID, mapping.PhoneNormalized, mapping.ViewerID, mapping.CreatedAt, mapping.UpdatedAt)
	return err
}

func (r *PostgresRepository) GetChatPhoneMappingByChatID(ctx context.Context, chatID string) (*domain.ChatPhoneMapping, error) {
	var m domain.ChatPhoneMapping
	var viewerID *int64
	err := r.pool.QueryRow(ctx, `
		SELECT id, chat_id, campaign_id, campaign_target_id, phone_normalized, viewer_id, created_at, updated_at
		FROM chat_phone_mappings
		WHERE chat_id = $1
	`, chatID).Scan(
		&m.ID, &m.ChatID, &m.CampaignID, &m.CampaignTargetID, &m.PhoneNormalized, &viewerID, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	m.ViewerID = viewerID
	return &m, nil
}
