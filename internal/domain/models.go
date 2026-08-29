package domain

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

type TenantType string

const (
	TenantTypeBeauty      TenantType = "beauty"
	TenantTypeAutoService TenantType = "auto_service"
	TenantTypeFitness     TenantType = "fitness"
	TenantTypeMedical     TenantType = "medical"
	TenantTypeRetail      TenantType = "retail"
	TenantTypeGeneric     TenantType = "generic"
)

type CampaignStatus string

const (
	CampaignStatusDraft      CampaignStatus = "draft"
	CampaignStatusProcessing CampaignStatus = "processing"
	CampaignStatusCompleted  CampaignStatus = "completed"
	CampaignStatusPaused     CampaignStatus = "paused"
	CampaignStatusFailed     CampaignStatus = "failed"
	CampaignStatusStopped    CampaignStatus = "stopped"
)

type TaskStatus string

const (
	TaskStatusPending             TaskStatus = "pending"
	TaskStatusSent                TaskStatus = "sent"
	TaskStatusDelivered           TaskStatus = "delivered"
	TaskStatusViewed              TaskStatus = "viewed"
	TaskStatusFailed              TaskStatus = "failed"
	TaskStatusReplied             TaskStatus = "replied"
	TaskStatusUserNotFoundByPhone TaskStatus = "user_not_found_by_phone"
)

const (
	DefaultMessengerType        = "MAX"
	OutboxEventSend             = "message.send"
	OutboxEventSendExistingChat = "message.send_existing_chat"
)

// StatusText returns Russian text for status
func (s TaskStatus) StatusText() string {
	switch s {
	case TaskStatusPending:
		return "Ожидает отправки"
	case TaskStatusSent:
		return "Отправлено"
	case TaskStatusDelivered:
		return "Доставлено"
	case TaskStatusViewed:
		return "Просмотрено"
	case TaskStatusFailed:
		return "Ошибка"
	case TaskStatusReplied:
		return "Ответ получен"
	case TaskStatusUserNotFoundByPhone:
		return "Пользователь не найден"
	default:
		return "Неизвестно"
	}
}

func (s TaskStatus) IsErrorStatus() bool {
	return s == TaskStatusFailed || s == TaskStatusUserNotFoundByPhone
}

// Rank is used so viewed/replied cannot be overwritten by an earlier status.
func (s TaskStatus) Rank() int {
	switch s {
	case TaskStatusReplied:
		return 4
	case TaskStatusViewed:
		return 3
	case TaskStatusSent, TaskStatusDelivered:
		return 2
	case TaskStatusPending:
		return 1
	default:
		return 0
	}
}

type Tenant struct {
	ID              uuid.UUID  `json:"id"`
	Name            string     `json:"name"`
	Type            TenantType `json:"type"`
	AdminPhone      string     `json:"admin_phone"`
	AdminMessenger  string     `json:"admin_messenger"`
	KeycloakGroupID *string    `json:"keycloak_group_id,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type Campaign struct {
	ID                      uuid.UUID      `json:"id"`
	TenantID                uuid.UUID      `json:"tenant_id"`
	Name                    string         `json:"name"`
	MessageTemplate         string         `json:"message_template"`
	Status                  CampaignStatus `json:"status"`
	OriginalExcelName       string         `json:"original_excel_name"`
	OriginalExcelPath       *string        `json:"-"` // Don't serialize to JSON
	ProcessedExcelPath      *string        `json:"-"` // Don't serialize to JSON
	AttachmentURL           *string        `json:"attachment_url,omitempty"`
	AttachmentName          *string        `json:"attachment_name,omitempty"`
	Deleted                 bool           `json:"deleted"`
	StartImmediately        bool           `json:"start_immediately"`
	TimeToStart             *time.Time     `json:"time_to_start"`
	ProcessedCount          int            `json:"processed_count"`
	TotalCount              int            `json:"total_count"`
	ErrorCount              int            `json:"error_count"`
	EstimatedDays           *int           `json:"estimated_days,omitempty"`
	ScheduledCompletionDate *time.Time     `json:"scheduled_completion_date,omitempty"`
	FallbackToVKAllowed     bool           `json:"fallback_to_vk_allowed"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

type CampaignTarget struct {
	ID              uuid.UUID  `json:"id"`
	CampaignID      uuid.UUID  `json:"campaign_id"`
	ClientName      string     `json:"client_name"`
	PhoneNormalized string     `json:"phone_normalized"`
	MessengerType   string     `json:"messenger_type"`
	ExcelRowIndex   int        `json:"excel_row_index"`
	Status          TaskStatus `json:"status"`
	LastError       *string    `json:"last_error,omitempty"`
	SentAt          *time.Time `json:"sent_at,omitempty"`
	RepliedAt       *time.Time `json:"replied_at,omitempty"`
	LastReplyText   *string    `json:"last_reply_text,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type OutboxMessage struct {
	ID          uuid.UUID  `json:"id"`
	EventType   string     `json:"event_type"`
	Payload     []byte     `json:"payload"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
}

type CampaignReply struct {
	ID               uuid.UUID `json:"id"`
	CampaignTargetID uuid.UUID `json:"campaign_target_id"`
	MessageText      string    `json:"message_text"`
	ReceivedAt       time.Time `json:"received_at"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type BlockedRecipient struct {
	TenantID        uuid.UUID `json:"tenant_id"`
	PhoneNormalized string    `json:"phone_normalized"`
	BlockedAt       time.Time `json:"blocked_at"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type ChatPhoneMapping struct {
	ID               uuid.UUID `json:"id"`
	ChatID           string    `json:"chat_id"`
	CampaignID       uuid.UUID `json:"campaign_id"`
	CampaignTargetID uuid.UUID `json:"campaign_target_id"`
	PhoneNormalized  string    `json:"phone_normalized"`
	TenantID         uuid.UUID `json:"tenant_id"`
	MessengerType    string    `json:"messenger_type"`
	ViewerID         *int64    `json:"viewer_id,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type TenantDailyQuota struct {
	TenantID          uuid.UUID  `json:"tenant_id"`
	QuotaDate         time.Time  `json:"quota_date"`
	ColdLimit         int        `json:"cold_limit"`
	ColdUsed          int        `json:"cold_used"`
	WarmUsed          int        `json:"warm_used"`
	LastColdPublishAt *time.Time `json:"last_cold_publish_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type PendingTargetForDosing struct {
	TargetID        uuid.UUID
	CampaignID      uuid.UUID
	TenantID        uuid.UUID
	ClientName      string
	PhoneNormalized string
	MessageTemplate string
	ChatID          string
	IsWarm          bool
	MessengerType   string
	AttachmentURL   *string
	AttachmentName  *string
}

type SendTaskPayload struct {
	TaskID         string  `json:"task_id"`
	CampaignID     string  `json:"campaign_id"`
	TenantID       string  `json:"tenant_id"`
	Messenger      string  `json:"messenger"`
	MessengerType  string  `json:"messenger_type,omitempty"`
	Phone          string  `json:"phone"`
	MessageText    string  `json:"message_text"`
	UseChatID      bool    `json:"use_chat_id"`
	ChatID         string  `json:"chat_id,omitempty"`
	ContactType    string  `json:"contact_type,omitempty"`
	AttachmentURL  *string `json:"attachment_url,omitempty"`
	AttachmentName *string `json:"attachment_name,omitempty"`
}

type TargetResult struct {
	TargetID      uuid.UUID  `json:"target_id"`
	CampaignID    uuid.UUID  `json:"campaign_id"`
	TenantID      uuid.UUID  `json:"tenant_id,omitempty"`
	PhoneNumber   string     `json:"phone_number"`
	Status        TaskStatus `json:"status"`
	ReplyText     *string    `json:"reply_text,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
	Timestamp     time.Time  `json:"timestamp"`
	ChatID        string     `json:"chat_id,omitempty"`
	MessengerType string     `json:"messenger_type,omitempty"`
}

func parseOptionalUUID(raw string) (uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(raw)
}

// UnmarshalJSON accepts omitted/empty UUID fields from the worker
// (admin notify results send target_id="" which encoding/json rejects).
func (r *TargetResult) UnmarshalJSON(data []byte) error {
	var raw struct {
		TargetID      string     `json:"target_id"`
		CampaignID    string     `json:"campaign_id"`
		TenantID      string     `json:"tenant_id"`
		PhoneNumber   string     `json:"phone_number"`
		Status        TaskStatus `json:"status"`
		ReplyText     *string    `json:"reply_text"`
		ErrorMessage  *string    `json:"error_message"`
		Timestamp     time.Time  `json:"timestamp"`
		ChatID        string     `json:"chat_id"`
		MessengerType string     `json:"messenger_type"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var err error
	if r.TargetID, err = parseOptionalUUID(raw.TargetID); err != nil {
		return err
	}
	if r.CampaignID, err = parseOptionalUUID(raw.CampaignID); err != nil {
		return err
	}
	if r.TenantID, err = parseOptionalUUID(raw.TenantID); err != nil {
		return err
	}
	r.PhoneNumber = raw.PhoneNumber
	r.Status = raw.Status
	r.ReplyText = raw.ReplyText
	r.ErrorMessage = raw.ErrorMessage
	r.Timestamp = raw.Timestamp
	r.ChatID = raw.ChatID
	r.MessengerType = raw.MessengerType
	return nil
}

// New DTOs for tenant admin notifications
type ClientReplyInfo struct {
	UserPhone string    `json:"user_phone"`
	UserName  string    `json:"user_name"`
	Message   string    `json:"message"`
	Time      time.Time `json:"time"`
}

type TenantAdminNotificationTask struct {
	TenantPhone string            `json:"tenant_phone"`
	TenantID    string            `json:"tenant_id,omitempty"`
	ChatID      string            `json:"chat_id,omitempty"`
	UseChatID   bool              `json:"use_chat_id,omitempty"`
	Replies     []ClientReplyInfo `json:"replies"`
}

type CampaignRepository interface {
	CreateCampaign(ctx context.Context, campaign *Campaign, targets []*CampaignTarget, outbox []*OutboxMessage) error
	GetCampaign(ctx context.Context, id uuid.UUID) (*Campaign, error)
	ListCampaigns(ctx context.Context, tenantID uuid.UUID) ([]*Campaign, error)
	UpdateCampaignStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	UpdateCampaign(ctx context.Context, campaign *Campaign) error
	GetCampaignTargets(ctx context.Context, campaignID uuid.UUID) ([]*CampaignTarget, error)
	GetCampaignTargetsWithStatus(ctx context.Context, campaignID uuid.UUID, statuses []TaskStatus) ([]*CampaignTarget, error)
	GetCampaignsByStatus(ctx context.Context, status CampaignStatus) ([]*Campaign, error)
	GetTargetsByStatus(ctx context.Context, campaignID uuid.UUID, status TaskStatus) ([]*CampaignTarget, error)
	UpdateTargetStatus(ctx context.Context, targetID uuid.UUID, status TaskStatus, lastError *string, sentAt *time.Time) (*Campaign, error)
	RegisterReply(ctx context.Context, campaignID uuid.UUID, phone string, text string, repliedAt time.Time) (*Campaign, error)
	CreateReply(ctx context.Context, reply *CampaignReply) error
	GetRepliesByCampaign(ctx context.Context, campaignID uuid.UUID) ([]*CampaignReply, error)
	GetTenantByID(ctx context.Context, tenantID uuid.UUID) (*Tenant, error)
	GetCampaignTargetByID(ctx context.Context, targetID uuid.UUID) (*CampaignTarget, error)
	GetActiveCampaignsReadyToStart(ctx context.Context) ([]*Campaign, error)
	StopCampaign(ctx context.Context, campaignID uuid.UUID) error
	// StartCampaign transitions a campaign from draft to processing.
	// The CampaignDoser then enqueues targets under per-tenant anti-ban limits.
	StartCampaign(ctx context.Context, campaignID uuid.UUID) error
	UpsertChatPhoneMapping(ctx context.Context, mapping *ChatPhoneMapping) error
	GetChatPhoneMappingByChatID(ctx context.Context, chatID string) (*ChatPhoneMapping, error)
	GetChatPhoneMappingByPhone(ctx context.Context, tenantID uuid.UUID, phone string, messengerType string) (*ChatPhoneMapping, error)
	UpsertAdminChatMapping(ctx context.Context, chatID string, tenantID uuid.UUID, phone string, messengerType string) error
	CountMappedPhones(ctx context.Context, tenantID uuid.UUID, messengerType string, phones []string) (int, error)
	GetTenantsWithProcessingCampaigns(ctx context.Context) ([]uuid.UUID, error)
	GetOrCreateTenantDailyQuota(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, coldMin, coldMax int) (*TenantDailyQuota, error)
	GetNextPendingWarmTarget(ctx context.Context, tenantID uuid.UUID) (*PendingTargetForDosing, error)
	GetNextPendingColdTarget(ctx context.Context, tenantID uuid.UUID) (*PendingTargetForDosing, error)
	CreateDosedOutboxMessage(ctx context.Context, tenantID uuid.UUID, eventType string, payload []byte, publishAt time.Time) error
	// TryReserveColdSlot atomically takes a cold send slot if the daily limit
	// and minInterval have been satisfied. Returns false when the tenant must wait.
	TryReserveColdSlot(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time, at time.Time, minInterval time.Duration) (bool, error)
	IncrementWarmUsed(ctx context.Context, tenantID uuid.UUID, quotaDate time.Time) error
}

type OutboxRepository interface {
	GetPendingMessages(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkAsProcessed(ctx context.Context, id uuid.UUID) error
}

type BlocklistRepository interface {
	AddBlockedRecipient(ctx context.Context, tenantID uuid.UUID, phoneNormalized string) error
	ListBlockedRecipients(ctx context.Context) ([]*BlockedRecipient, error)
}
