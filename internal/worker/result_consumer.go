package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/usecase"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

const (
	batchSize      = 5
	flushInterval  = 55 * time.Second
)

type ResultConsumer struct {
	repo       domain.CampaignRepository
	uc         *usecase.CampaignUseCase
	amqpConn   *amqp091.Connection
	amqpChan   *amqp091.Channel // Added back amqpChan
	queueName  string
	mu         sync.Mutex
	isRunning  bool
	results    []domain.TargetResult
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

func NewResultConsumer(repo domain.CampaignRepository, uc *usecase.CampaignUseCase, amqpConn *amqp091.Connection, queueName string) (*ResultConsumer, error) {
	rc := &ResultConsumer{
		repo:      repo,
		uc:        uc,
		amqpConn: amqpConn,
		queueName: queueName,
	}
	err := rc.reconnect()
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (rc *ResultConsumer) reconnect() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.amqpChan != nil {
		_ = rc.amqpChan.Close()
	}

	ch, err := rc.amqpConn.Channel()
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		rc.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	rc.amqpChan = ch
	log.Println("ResultConsumer: reconnected to RabbitMQ channel")
	return nil
}

func (rc *ResultConsumer) Run(ctx context.Context) error {
	rc.mu.Lock()
	if rc.isRunning {
		rc.mu.Unlock()
		log.Println("ResultConsumer: already running")
		return nil
	}
	rc.isRunning = true
	rc.mu.Unlock()

	defer func() {
		rc.mu.Lock()
		rc.isRunning = false
		rc.mu.Unlock()
	}()

	rc.wg.Add(1)
	go rc.flushLoop(ctx)

	ch, err := rc.amqpConn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		rc.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		rc.queueName,
		"result-consumer",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Println("ResultConsumer: started consuming messages")

	for {
		select {
		case <-ctx.Done():
			close(rc.stopChan)
			rc.wg.Wait()
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				log.Println("ResultConsumer: channel closed, exiting")
				return nil
			}

			if len(msg.Body) == 0 {
				log.Println("ResultConsumer: skipping empty message")
				continue
			}

			var result domain.TargetResult
			err := json.Unmarshal(msg.Body, &result)
			if err != nil {
				log.Printf("ResultConsumer: failed to unmarshal message (skipping): %v, body: %s", err, string(msg.Body))
				continue
			}

			rc.mu.Lock()
			rc.results = append(rc.results, result)
			count := len(rc.results)
			rc.mu.Unlock()

			log.Printf("ResultConsumer: received result for target %s, current batch size: %d", result.TargetID, count)

			if count >= batchSize {
				rc.flush(ctx)
			}
		}
	}
}

func (rc *ResultConsumer) flushLoop(ctx context.Context) {
	defer rc.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("ResultConsumer: flushing due to timeout")
			rc.flush(ctx)
		case <-rc.stopChan:
			log.Println("ResultConsumer: stop signal received, final flush")
			rc.flush(ctx)
			return
		case <-ctx.Done():
			log.Println("ResultConsumer: context canceled, final flush")
			rc.flush(ctx)
			return
		}
	}
}

func (rc *ResultConsumer) flush(ctx context.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if len(rc.results) == 0 {
		return
	}

	toProcess := make([]domain.TargetResult, len(rc.results))
	copy(toProcess, rc.results)
	rc.results = nil

	log.Printf("ResultConsumer: processing batch of %d results", len(toProcess))

	// Track unique campaign IDs to regenerate Excel files for
	uniqueCampaignIDs := make(map[uuid.UUID]struct{})
	// Track replies grouped by tenant
	tenantReplies := make(map[uuid.UUID][]domain.ClientReplyInfo)

	for _, result := range toProcess {
		processCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		log.Printf("ResultConsumer: processing result for target %s, status: %s, reply: %v", result.TargetID, result.Status, result.ReplyText)

		if result.ReplyText != nil && result.Status == "replied" {
			// First, register the reply in DB
			campaign, err := rc.repo.RegisterReply(processCtx, result.CampaignID, result.PhoneNumber, *result.ReplyText, result.Timestamp)
			if err != nil {
				log.Printf("ResultConsumer: failed to register reply for target %s: %v", result.TargetID, err)
				continue
			}

			uniqueCampaignIDs[result.CampaignID] = struct{}{}

			// Now, get the CampaignTarget to retrieve ClientName
			target, err := rc.repo.GetCampaignTargetByID(processCtx, result.TargetID)
			if err != nil {
				log.Printf("ResultConsumer: failed to get target %s: %v", result.TargetID, err)
				continue
			}

			// Add to tenantReplies
			tenantReplies[campaign.TenantID] = append(tenantReplies[campaign.TenantID], domain.ClientReplyInfo{
				UserPhone: target.PhoneNormalized,
				UserName:  target.ClientName,
				Message:   *result.ReplyText,
				Time:      result.Timestamp,
			})
		} else if result.Status != "" {
			_, err := rc.repo.UpdateTargetStatus(processCtx, result.TargetID, domain.TaskStatus(result.Status), nil, nil)
			if err != nil {
				log.Printf("ResultConsumer: failed to update target %s status to %s: %v", result.TargetID, result.Status, err)
			} else {
				uniqueCampaignIDs[result.CampaignID] = struct{}{}
			}
		}
	}

	// Now regenerate Excel files for each unique campaign
	for campaignID := range uniqueCampaignIDs {
		log.Printf("ResultConsumer: regenerating Excel for campaign %s", campaignID)
		excelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		_, err := rc.uc.GenerateExcel(excelCtx, campaignID)
		if err != nil {
			log.Printf("ResultConsumer: failed to generate Excel for campaign %s: %v", campaignID, err)
		}
	}

	// Now, send tenant admin notifications
	for tenantID, replies := range tenantReplies {
		if len(replies) == 0 {
			continue
		}

		tenantCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tenant, err := rc.repo.GetTenantByID(tenantCtx, tenantID)
		if err != nil {
			log.Printf("ResultConsumer: failed to get tenant %s: %v", tenantID, err)
			continue
		}

		if tenant.AdminPhone == "" {
			log.Printf("ResultConsumer: tenant %s has no admin phone, skipping notification", tenantID)
			continue
		}

		// Prepare the task
		task := domain.TenantAdminNotificationTask{
			TenantPhone: tenant.AdminPhone,
			Replies:     replies,
		}

		// Serialize to JSON
		payload, err := json.Marshal(task)
		if err != nil {
			log.Printf("ResultConsumer: failed to serialize notification task: %v", err)
			continue
		}

		// Publish to RabbitMQ
		publishCtx, cancelPublish := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelPublish()

		// Check if channel is still alive, reconnect if needed
		if rc.amqpChan == nil {
			log.Println("ResultConsumer: channel is nil, trying to reconnect")
			if err := rc.reconnect(); err != nil {
				log.Printf("ResultConsumer: failed to reconnect: %v", err)
				continue
			}
		}

		err = rc.amqpChan.PublishWithContext(
			publishCtx,
			"",                                  // exchange
			"tasks.messages.tenant_admin_notify", // routing key
			false,                               // mandatory
			false,                               // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        payload,
			},
		)
		if err != nil {
			log.Printf("ResultConsumer: failed to publish notification: %v, trying to reconnect", err)
			if err := rc.reconnect(); err == nil {
				// Try once more after reconnect
				err = rc.amqpChan.PublishWithContext(
					publishCtx,
					"",
					"tasks.messages.tenant_admin_notify",
					false,
					false,
					amqp091.Publishing{
						ContentType: "application/json",
						Body:        payload,
					},
				)
				if err != nil {
					log.Printf("ResultConsumer: still failed to publish notification: %v", err)
				} else {
					log.Printf("ResultConsumer: published notification to tenant %s (%d replies)", tenantID, len(replies))
				}
			} else {
				log.Printf("ResultConsumer: failed to reconnect: %v", err)
			}
		} else {
			log.Printf("ResultConsumer: published notification to tenant %s (%d replies)", tenantID, len(replies))
		}
	}
}
