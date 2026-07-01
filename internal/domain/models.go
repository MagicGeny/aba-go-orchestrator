package domain

import (
	"context"
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
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusSent      TaskStatus = "sent"
	TaskStatusDelivered TaskStatus = "delivered"
	TaskStatusViewed    TaskStatus = "viewed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusReplied   TaskStatus = "replied"
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
	default:
		return "Неизвестно"
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
	ID                 uuid.UUID      `json:"id"`
	TenantID           uuid.UUID      `json:"tenant_id"`
	Name               string         `json:"name"`
	MessageTemplate    string         `json:"message_template"`
	Status             CampaignStatus `json:"status"`
	OriginalExcelName  string         `json:"original_excel_name"`
	OriginalExcelPath  *string        `json:"-"` // Don't serialize to JSON
	ProcessedExcelPath *string        `json:"-"` // Don't serialize to JSON
	Deleted            bool           `json:"deleted"`
	ProcessedCount     int            `json:"processed_count"`
	TotalCount         int            `json:"total_count"`
	ErrorCount         int            `json:"error_count"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type CampaignTarget struct {
	ID              uuid.UUID  `json:"id"`
	CampaignID      uuid.UUID  `json:"campaign_id"`
	ClientName      string     `json:"client_name"`
	PhoneNormalized string     `json:"phone_normalized"`
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

type TargetResult struct {
	TargetID    uuid.UUID  `json:"target_id"`
	CampaignID  uuid.UUID  `json:"campaign_id"`
	PhoneNumber string     `json:"phone_number"`
	Status      TaskStatus `json:"status"`
	ReplyText   *string    `json:"reply_text,omitempty"`
	Timestamp   time.Time  `json:"timestamp"`
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
}

type OutboxRepository interface {
	GetPendingMessages(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkAsProcessed(ctx context.Context, id uuid.UUID) error
}
