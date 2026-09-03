package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/usecase"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

const (
	batchSize     = 1
	flushInterval = 55 * time.Second
)

type BlocklistUpdater interface {
	Add(tenantID uuid.UUID, phoneNormalized string)
}

type ResultConsumer struct {
	repo      domain.CampaignRepository
	uc        *usecase.CampaignUseCase
	amqpConn  *amqp091.Connection
	amqpChan  *amqp091.Channel // Added back amqpChan
	queueName string
	blocklist BlocklistUpdater
	mu        sync.Mutex
	isRunning bool
	results   []queuedResult
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

type queuedResult struct {
	result domain.TargetResult
	msg    amqp091.Delivery
}

func NewResultConsumer(repo domain.CampaignRepository, uc *usecase.CampaignUseCase, amqpConn *amqp091.Connection, queueName string, blocklist BlocklistUpdater) (*ResultConsumer, error) {
	rc := &ResultConsumer{
		repo:      repo,
		uc:        uc,
		amqpConn:  amqpConn,
		queueName: queueName,
		blocklist: blocklist,
		stopChan:  make(chan struct{}),
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
		false,
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
				_ = msg.Ack(false)
				continue
			}

			log.Printf("ResultConsumer: received message body: %s", string(msg.Body))

			var result domain.TargetResult
			err := json.Unmarshal(msg.Body, &result)
			if err != nil {
				log.Printf("ResultConsumer: failed to unmarshal message (skipping): %v, body: %s", err, string(msg.Body))
				_ = msg.Ack(false)
				continue
			}

			rc.mu.Lock()
			rc.results = append(rc.results, queuedResult{result: result, msg: msg})
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

	toProcess := make([]queuedResult, len(rc.results))
	copy(toProcess, rc.results)
	rc.results = nil

	log.Printf("ResultConsumer: processing batch of %d results", len(toProcess))

	// Track replies grouped by tenant
	tenantReplies := make(map[uuid.UUID][]domain.ClientReplyInfo)

	for _, item := range toProcess {
		result := item.result
		processCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		log.Printf("ResultConsumer: processing result for target %s, status: %s, reply: %v", result.TargetID, result.Status, result.ReplyText)

		processedOK := true

		if result.ChatID != "" {
			if result.Status == domain.TaskStatusSent && result.TargetID != uuid.Nil && result.CampaignID != uuid.Nil && strings.TrimSpace(result.PhoneNumber) != "" {
				err := rc.repo.UpsertChatPhoneMapping(processCtx, &domain.ChatPhoneMapping{
					ChatID:           result.ChatID,
					CampaignID:       result.CampaignID,
					CampaignTargetID: result.TargetID,
					PhoneNormalized:  result.PhoneNumber,
				})
				if err != nil {
					log.Printf("ResultConsumer: failed to upsert chat mapping for target %s chat_id=%s: %v", result.TargetID, result.ChatID, err)
				}
			} else if result.Status == domain.TaskStatusSent && result.TenantID != uuid.Nil && strings.TrimSpace(result.PhoneNumber) != "" {
				// Admin notification: TargetID/CampaignID is empty, but we know the tenant_id and phone number.
				// Save the chat_id so that future notifications will be sent directly to the chat_id.
				if err := rc.repo.UpsertAdminChatMapping(processCtx, result.ChatID, result.TenantID, result.PhoneNumber, result.MessengerType); err != nil {
					log.Printf("ResultConsumer: failed to upsert admin chat mapping for chat_id=%s phone=%s: %v", result.ChatID, result.PhoneNumber, err)
				}
			}

			if result.TargetID == uuid.Nil || result.CampaignID == uuid.Nil || strings.TrimSpace(result.PhoneNumber) == "" {
				mapping, err := rc.repo.GetChatPhoneMappingByChatID(processCtx, result.ChatID)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						log.Printf("ResultConsumer: chat_id mapping not found (skipping): chat_id=%s status=%s", result.ChatID, result.Status)
					} else {
						log.Printf("ResultConsumer: failed to resolve chat_id mapping (skipping): chat_id=%s err=%v", result.ChatID, err)
					}
					cancel()
					_ = item.msg.Ack(false)
					continue
				}
				result.TargetID = mapping.CampaignTargetID
				result.CampaignID = mapping.CampaignID
				result.PhoneNumber = mapping.PhoneNormalized
				if mapping.CampaignTargetID == uuid.Nil {
					log.Printf("ResultConsumer: chat_id=%s maps to admin/non-campaign row (skipping status=%s)", result.ChatID, result.Status)
					cancel()
					_ = item.msg.Ack(false)
					continue
				}
			}
		}

		if result.Status == domain.TaskStatusSent && result.TenantID != uuid.Nil && result.TargetID == uuid.Nil && result.CampaignID == uuid.Nil {
			cancel()
			if err := item.msg.Ack(false); err != nil {
				log.Printf("ResultConsumer: failed to ack message: %v", err)
			}
			continue
		}

		if result.TargetID == uuid.Nil || result.CampaignID == uuid.Nil {
			if result.ChatID != "" {
				log.Printf("ResultConsumer: unresolved mapping (skipping): chat_id=%s status=%s", result.ChatID, result.Status)
			} else {
				log.Printf("ResultConsumer: unresolved mapping (skipping): target=%s campaign=%s status=%s", result.TargetID, result.CampaignID, result.Status)
			}
			cancel()
			_ = item.msg.Ack(false)
			continue
		}

		if result.ReplyText != nil && result.Status == domain.TaskStatusReplied {
			if existingTarget, err := rc.repo.GetCampaignTargetByID(processCtx, result.TargetID); err == nil && existingTarget != nil {
				if existingTarget.Status == domain.TaskStatusReplied && existingTarget.LastReplyText != nil && existingTarget.RepliedAt != nil {
					if strings.TrimSpace(*existingTarget.LastReplyText) == strings.TrimSpace(*result.ReplyText) {
						d := existingTarget.RepliedAt.Sub(result.Timestamp)
						if d < 0 {
							d = -d
						}
						if d <= 2*time.Minute {
							log.Printf("ResultConsumer: duplicate reply detected (skipping): target=%s chat_id=%s", result.TargetID, result.ChatID)
							cancel()
							if err := item.msg.Ack(false); err != nil {
								log.Printf("ResultConsumer: failed to ack message: %v", err)
							}
							continue
						}
					}
				}
			}

			campaign, err := rc.repo.RegisterReply(processCtx, result.CampaignID, result.PhoneNumber, *result.ReplyText, result.Timestamp)
			if err != nil {
				log.Printf("ResultConsumer: failed to register reply for target %s: %v", result.TargetID, err)
				processedOK = false
				cancel()
				goto finish
			}

			target, err := rc.repo.GetCampaignTargetByID(processCtx, result.TargetID)
			if err != nil {
				log.Printf("ResultConsumer: failed to get target %s: %v", result.TargetID, err)
				processedOK = false
				cancel()
				goto finish
			}

			if strings.TrimSpace(*result.ReplyText) == "@" && rc.blocklist != nil {
				rc.blocklist.Add(campaign.TenantID, target.PhoneNormalized)
			}

			// Add to tenantReplies
			tenantReplies[campaign.TenantID] = append(tenantReplies[campaign.TenantID], domain.ClientReplyInfo{
				UserPhone: target.PhoneNormalized,
				UserName:  target.ClientName,
				Message:   *result.ReplyText,
				Time:      result.Timestamp,
			})
		} else if result.Status == domain.TaskStatusViewed {
			// Handle "viewed" status - update target status and record viewed time
			_, err := rc.repo.UpdateTargetStatus(processCtx, result.TargetID, domain.TaskStatus(result.Status), result.ErrorMessage, nil)
			if err != nil {
				log.Printf("ResultConsumer: failed to update target %s status to %s: %v", result.TargetID, result.Status, err)
				processedOK = false
			}
		} else if result.Status == domain.TaskStatusUserNotFoundByPhone {
			log.Printf("ResultConsumer: USER_NOT_FOUND_BY_PHONE for target %s (cold search already counted in tenant quota at enqueue)", result.TargetID)
			_, err := rc.repo.UpdateTargetStatus(processCtx, result.TargetID, domain.TaskStatusUserNotFoundByPhone, result.ErrorMessage, nil)
			if err != nil {
				log.Printf("ResultConsumer: failed to update target %s status to %s: %v", result.TargetID, result.Status, err)
				processedOK = false
			}
		} else if result.Status != "" {
			_, err := rc.repo.UpdateTargetStatus(processCtx, result.TargetID, domain.TaskStatus(result.Status), result.ErrorMessage, nil)
			if err != nil {
				log.Printf("ResultConsumer: failed to update target %s status to %s: %v", result.TargetID, result.Status, err)
				processedOK = false
			}
		}
		cancel()

	finish:
		if processedOK {
			if err := item.msg.Ack(false); err != nil {
				log.Printf("ResultConsumer: failed to ack message: %v", err)
			}
		} else {
			if err := item.msg.Nack(false, true); err != nil {
				log.Printf("ResultConsumer: failed to nack message: %v", err)
			}
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

		// Parse multiple admin phone numbers separated by semicolons
		adminPhones := domain.ParseAdminPhones(tenant.AdminPhone)
		if len(adminPhones) == 0 {
			log.Printf("ResultConsumer: tenant %s has no valid admin phone numbers after parsing, skipping notification", tenantID)
			continue
		}

		// For each admin phone number, create a separate notification task
		for _, adminPhone := range adminPhones {
			// Try to find a saved chat_id for this specific admin phone number.
			// If there is one, we send it directly to the chat_id (use_chat_id=true).
			// If not, we send it to the number (use_chat_id=false). The worker will save the chat_id automatically if successful.
			adminNormalized := domain.NormalizePhone(adminPhone)
			var adminChatID string
			var adminUseChatID bool
			if adminNormalized != "" {
				mt := string(domain.DefaultMessengerType)
				if mapping, err := rc.repo.GetChatPhoneMappingByPhone(tenantCtx, tenantID, adminNormalized, mt); err == nil && mapping != nil && mapping.ChatID != "" {
					adminChatID = mapping.ChatID
					adminUseChatID = true
					log.Printf("ResultConsumer: admin chat_id found in mappings: chat_id=%s tenant=%s phone=%s", adminChatID, tenantID, adminPhone)
				} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					log.Printf("ResultConsumer: admin chat_id lookup failed (fallback to phone) tenant=%s phone=%s: %v", tenantID, adminPhone, err)
				}
			}

			// Prepare the task for this specific phone number
			task := domain.TenantAdminNotificationTask{
				TenantID:    tenantID.String(),
				TenantPhone: adminPhone,
				ChatID:      adminChatID,
				UseChatID:   adminUseChatID,
				Replies:     replies,
			}

			// Serialize to JSON
			payload, err := json.Marshal(task)
			if err != nil {
				log.Printf("ResultConsumer: failed to serialize notification task for tenant %s phone %s: %v", tenantID, adminPhone, err)
				continue
			}

			delaySec := 45 + rand.IntN(31)
			log.Printf("ResultConsumer: sleeping for %d seconds before notifying admin (%s)", delaySec, adminPhone)

			select {
			case <-ctx.Done():
				log.Printf("ResultConsumer: context canceled during admin notify delay for tenant %s", tenantID)
				return
			case <-time.After(time.Duration(delaySec) * time.Second):
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
				"",                                   // exchange
				"tasks.messages.tenant_admin_notify", // routing key
				false,                                // mandatory
				false,                                // immediate
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        payload,
				},
			)
			if err != nil {
				log.Printf("ResultConsumer: failed to publish notification for tenant %s phone %s: %v, trying to reconnect", tenantID, adminPhone, err)
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
						log.Printf("ResultConsumer: still failed to publish notification for tenant %s phone %s: %v", tenantID, adminPhone, err)
					} else {
						log.Printf("ResultConsumer: published notification to tenant %s phone %s (%d replies, chat_id=%s, use_chat_id=%v)", tenantID, adminPhone, len(replies), adminChatID, adminUseChatID)
					}
				} else {
					log.Printf("ResultConsumer: failed to reconnect for tenant %s phone %s: %v", tenantID, adminPhone, err)
				}
			} else {
				log.Printf("ResultConsumer: published notification to tenant %s phone %s (%d replies, chat_id=%s, use_chat_id=%v)", tenantID, adminPhone, len(replies), adminChatID, adminUseChatID)
			}
		}
	}
}
