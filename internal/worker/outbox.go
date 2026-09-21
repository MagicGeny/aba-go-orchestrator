package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type BlockChecker interface {
	IsBlocked(tenantID uuid.UUID, phoneNormalized string) bool
}

type OutboxWorker struct {
	repo          domain.OutboxRepository
	amqpConn      *amqp091.Connection
	amqpChan      *amqp091.Channel
	queueName     string
	warmQueueName string
	resultsQueue  string
	sendExchange  string
	blockChecker  BlockChecker
	cfg           config.Config
}

const (
	QueueSend             = "tasks.messages.send"
	QueueSendExistingChat = "tasks.messages.send_existing_chat"
)

// AccountRoutingKey builds the Direct Exchange routing key from tenant_account_id (UUID).
func AccountRoutingKey(tenantAccountID string) string {
	return "account." + tenantAccountID
}

// AccountSendQueue builds the per-account send queue name from tenant_account_id (UUID).
func AccountSendQueue(tenantAccountID string) string {
	return "tasks.messages.send.account." + tenantAccountID
}

func NewOutboxWorker(repo domain.OutboxRepository, amqpConn *amqp091.Connection, queueName string, warmQueueName string, resultsQueue string, blockChecker BlockChecker, cfg config.Config) (*OutboxWorker, error) {
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}

	sendExchange := strings.TrimSpace(cfg.RabbitMQSendExchange)
	if sendExchange == "" {
		sendExchange = "tasks.messages.direct"
	}
	if err := ch.ExchangeDeclare(
		sendExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return nil, fmt.Errorf("declare send exchange %s: %w", sendExchange, err)
	}

	// Confirm mode: mark outbox processed only after broker ack when supported.
	if err := ch.Confirm(false); err != nil {
		log.Printf("[outbox] publisher confirms unavailable (%v); falling back to fire-and-forget publish", err)
	}

	for _, q := range []string{queueName, warmQueueName, resultsQueue} {
		if q == "" {
			continue
		}
		_, err = ch.QueueDeclare(
			q,
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return nil, err
		}
	}

	return &OutboxWorker{
		repo:          repo,
		amqpConn:      amqpConn,
		amqpChan:      ch,
		queueName:     queueName,
		warmQueueName: warmQueueName,
		resultsQueue:  resultsQueue,
		sendExchange:  sendExchange,
		blockChecker:  blockChecker,
		cfg:           cfg,
	}, nil
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processMessages(ctx)
		}
	}
}

func (w *OutboxWorker) ensureAccountQueue(tenantAccountID string) (queueName string, routingKey string, err error) {
	routingKey = AccountRoutingKey(tenantAccountID)
	queueName = AccountSendQueue(tenantAccountID)
	_, err = w.amqpChan.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return "", "", err
	}
	if err := w.amqpChan.QueueBind(queueName, routingKey, w.sendExchange, false, nil); err != nil {
		return "", "", err
	}
	return queueName, routingKey, nil
}

func resolveTenantAccountID(msg *domain.OutboxMessage, payloadTenantAccountID, payloadAccountID string) string {
	if msg.TenantAccountID != uuid.Nil {
		return msg.TenantAccountID.String()
	}
	if id := strings.TrimSpace(payloadTenantAccountID); id != "" {
		return id
	}
	// Legacy payloads may have put the UUID only in account_id.
	if id := strings.TrimSpace(payloadAccountID); id != "" {
		if _, err := uuid.Parse(id); err == nil {
			return id
		}
	}
	return ""
}

func (w *OutboxWorker) processMessages(ctx context.Context) {
	messages, err := w.repo.GetPendingMessages(ctx, 100)
	if err != nil {
		log.Printf("failed to get pending messages: %v", err)
		return
	}

	processedIDs := make([]uuid.UUID, 0, len(messages))

	for _, msg := range messages {
		var sendTask struct {
			TaskID          string  `json:"task_id"`
			CampaignID      string  `json:"campaign_id"`
			TenantID        string  `json:"tenant_id"`
			TenantAccountID string  `json:"tenant_account_id"`
			AccountID       string  `json:"account_id"`
			Messenger       string  `json:"messenger"`
			Phone           string  `json:"phone"`
			MessageText     string  `json:"message_text"`
			UseChatID       bool    `json:"use_chat_id"`
			ChatID          string  `json:"chat_id"`
			ContactType     string  `json:"contact_type"`
			AttachmentURL   *string `json:"attachment_url,omitempty"`
			AttachmentName  *string `json:"attachment_name,omitempty"`
		}
		_ = json.Unmarshal(msg.Payload, &sendTask)

		if strings.EqualFold(sendTask.ContactType, "cold") && !w.cfg.IsWithinWorkWindow(time.Now()) {
			log.Printf("[outbox] deferring cold message id=%s phone=%s until work window %s-%s %s",
				msg.ID, sendTask.Phone, w.cfg.WorkWindowStart, w.cfg.WorkWindowEnd, w.cfg.Location)
			continue
		}
		if w.blockChecker != nil && sendTask.TenantID != "" && sendTask.Phone != "" {
			tenantID, errTenant := uuid.Parse(sendTask.TenantID)
			if errTenant == nil && w.blockChecker.IsBlocked(tenantID, sendTask.Phone) {
				taskID, errTask := uuid.Parse(sendTask.TaskID)
				campaignID, errCampaign := uuid.Parse(sendTask.CampaignID)
				if errTask == nil && errCampaign == nil {
					errorMessage := "Заблокировано пользователем"
					errorCode := domain.ErrorCodeBlockedRecipient
					result := domain.TargetResult{
						TargetID:     taskID,
						CampaignID:   campaignID,
						PhoneNumber:  sendTask.Phone,
						Status:       domain.TaskStatusFailed,
						ErrorCode:    errorCode,
						ErrorMessage: &errorMessage,
						AccountID:    sendTask.AccountID,
						Timestamp:    time.Now().UTC(),
					}
					if sendTask.TenantAccountID != "" {
						if aid, err := uuid.Parse(sendTask.TenantAccountID); err == nil {
							result.TenantAccountID = aid
						}
					}
					body, errMarshal := json.Marshal(result)
					if errMarshal == nil {
						err = w.amqpChan.PublishWithContext(ctx,
							"",
							w.resultsQueue,
							false,
							false,
							amqp091.Publishing{
								ContentType: "application/json",
								Body:        body,
							},
						)
						if err != nil {
							log.Printf("failed to publish blocked result for message %s: %v", msg.ID, err)
							continue
						}
						processedIDs = append(processedIDs, msg.ID)
						continue
					}
				}
			}
		}

		tenantAccountID := resolveTenantAccountID(msg, sendTask.TenantAccountID, sendTask.AccountID)
		if tenantAccountID == "" {
			log.Printf("[outbox] skipping message id=%s: missing tenant_account_id (cannot route)", msg.ID)
			continue
		}

		queueName, routingKey, ensureErr := w.ensureAccountQueue(tenantAccountID)
		if ensureErr != nil {
			log.Printf("failed to ensure account queue for %s: %v", tenantAccountID, ensureErr)
			continue
		}
		log.Printf("[outbox] publishing message id=%s exchange=%s routing_key=%s queue=%s task_id=%s campaign_id=%s phone=%s use_chat_id=%v tenant_account_id=%s attachment_url=%q attachment_name=%q",
			msg.ID, w.sendExchange, routingKey, queueName, sendTask.TaskID, sendTask.CampaignID, sendTask.Phone, sendTask.UseChatID,
			tenantAccountID, strDeref(sendTask.AttachmentURL), strDeref(sendTask.AttachmentName))

		confirmation, pubErr := w.amqpChan.PublishWithDeferredConfirmWithContext(ctx,
			w.sendExchange,
			routingKey,
			false,
			false,
			amqp091.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp091.Persistent,
				Body:         msg.Payload,
			},
		)
		if pubErr != nil {
			log.Printf("failed to publish message %s: %v", msg.ID, pubErr)
			continue
		}
		if confirmation != nil {
			acked, confErr := confirmation.WaitContext(ctx)
			if confErr != nil {
				log.Printf("failed waiting for publish confirm message %s: %v", msg.ID, confErr)
				continue
			}
			if !acked {
				log.Printf("broker nacked message %s — leaving outbox pending", msg.ID)
				continue
			}
		}

		processedIDs = append(processedIDs, msg.ID)
	}

	if len(processedIDs) == 0 {
		return
	}

	type processedBatchRepo interface {
		MarkAsProcessedBatch(ctx context.Context, ids []uuid.UUID) error
	}
	if br, ok := w.repo.(processedBatchRepo); ok {
		if err := br.MarkAsProcessedBatch(ctx, processedIDs); err != nil {
			log.Printf("failed to mark %d messages as processed (batch): %v", len(processedIDs), err)
		}
		return
	}

	for _, id := range processedIDs {
		if err := w.repo.MarkAsProcessed(ctx, id); err != nil {
			log.Printf("failed to mark message %s as processed: %v", id, err)
		}
	}
}

func strDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
