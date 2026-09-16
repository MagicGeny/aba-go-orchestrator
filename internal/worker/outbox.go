package worker

import (
	"context"
	"encoding/json"
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
	blockChecker  BlockChecker
	cfg           config.Config
}

const (
	QueueSend             = "tasks.messages.send"
	QueueSendExistingChat = "tasks.messages.send_existing_chat"
)

func NewOutboxWorker(repo domain.OutboxRepository, amqpConn *amqp091.Connection, queueName string, warmQueueName string, resultsQueue string, blockChecker BlockChecker, cfg config.Config) (*OutboxWorker, error) {
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, err
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

func (w *OutboxWorker) processMessages(ctx context.Context) {
	messages, err := w.repo.GetPendingMessages(ctx, 100)
	if err != nil {
		log.Printf("failed to get pending messages: %v", err)
		return
	}

	processedIDs := make([]uuid.UUID, 0, len(messages))

	for _, msg := range messages {
		var sendTask struct {
			TaskID         string  `json:"task_id"`
			CampaignID     string  `json:"campaign_id"`
			TenantID       string  `json:"tenant_id"`
			Messenger      string  `json:"messenger"`
			Phone          string  `json:"phone"`
			MessageText    string  `json:"message_text"`
			UseChatID      bool    `json:"use_chat_id"`
			ChatID         string  `json:"chat_id"`
			ContactType    string  `json:"contact_type"`
			AttachmentURL  *string `json:"attachment_url,omitempty"`
			AttachmentName *string `json:"attachment_name,omitempty"`
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
					result := domain.TargetResult{
						TargetID:     taskID,
						CampaignID:   campaignID,
						PhoneNumber:  sendTask.Phone,
						Status:       domain.TaskStatusFailed,
						ErrorMessage: &errorMessage,
						Timestamp:    time.Now().UTC(),
					}
					body, errMarshal := json.Marshal(result)
					if errMarshal == nil {
						err = w.amqpChan.PublishWithContext(ctx,
							"",             // exchange
							w.resultsQueue, // routing key
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

		routingKey := w.queueName
		if sendTask.UseChatID || sendTask.ContactType == "warm" {
			if w.warmQueueName != "" {
				routingKey = w.warmQueueName
			}
		}

		log.Printf("[outbox] publishing message id=%s queue=%s task_id=%s campaign_id=%s phone=%s use_chat_id=%v attachment_url=%q attachment_name=%q payload=%s",
			msg.ID, routingKey, sendTask.TaskID, sendTask.CampaignID, sendTask.Phone, sendTask.UseChatID,
			strDeref(sendTask.AttachmentURL), strDeref(sendTask.AttachmentName), string(msg.Payload))
		pubErr := w.amqpChan.PublishWithContext(ctx,
			"",         // exchange
			routingKey, // routing key
			false,      // mandatory
			false,      // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        msg.Payload,
			},
		)
		if pubErr != nil {
			log.Printf("failed to publish message %s: %v", msg.ID, pubErr)
			continue
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
