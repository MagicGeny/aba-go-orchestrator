package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/logging"
	"github.com/MagicGeny/aba-go-orchestrator/internal/usecase"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rabbitmq/amqp091-go"
)

const (
	batchSize     = 1
	flushInterval = 55 * time.Second
)

// notifyGroupKey identifies one tenant-admin notification group. Replies are
// grouped by BOTH the tenant and the sender account (tenant_accounts.id): MAX
// chats belong to one account, so grouping by tenant alone would let account B
// send (and persist a chat mapping for) account A's replies.
type notifyGroupKey struct {
	TenantID        uuid.UUID
	TenantAccountID uuid.UUID
}

// observedReply is a reply together with its account provenance.
type observedReply struct {
	TenantID        uuid.UUID
	TenantAccountID uuid.UUID
	Reply           domain.ClientReplyInfo
}

// groupRepliesByTenantAccount groups replies by (tenant_id, tenant_account_id).
func groupRepliesByTenantAccount(observed []observedReply) map[notifyGroupKey][]domain.ClientReplyInfo {
	groups := make(map[notifyGroupKey][]domain.ClientReplyInfo)
	for _, item := range observed {
		key := notifyGroupKey{TenantID: item.TenantID, TenantAccountID: item.TenantAccountID}
		groups[key] = append(groups[key], item.Reply)
	}
	return groups
}

// replyAccountID resolves which sender account observed a reply. The worker
// reports tenant_account_id for its own events; polled replies are attributed to
// the account assigned to the campaign target when it was dosed. uuid.Nil means
// the account is unknown: such a reply must not produce an account-ambiguous
// admin notification.
func replyAccountID(resultTenantAccountID uuid.UUID, target *domain.CampaignTarget) uuid.UUID {
	if resultTenantAccountID != uuid.Nil {
		return resultTenantAccountID
	}
	if target != nil && target.TenantAccountID != nil {
		return *target.TenantAccountID
	}
	return uuid.Nil
}

// resultAccountID resolves the sender account reported in a worker result.
// tenant_account_id is the authoritative field; account_id is the legacy one.
func resultAccountID(result domain.TargetResult) uuid.UUID {
	if result.TenantAccountID != uuid.Nil {
		return result.TenantAccountID
	}
	if id := strings.TrimSpace(result.AccountID); id != "" {
		if parsed, err := uuid.Parse(id); err == nil {
			return parsed
		}
	}
	return uuid.Nil
}

// resultTrace builds the correlation identifier set used by the worker-result
// diagnostics. It mirrors the identifiers used when the task was published
// (WORKER_TASK_PUBLISH_*) so a single target can be followed end to end.
func resultTrace(result *domain.TargetResult) map[string]any {
	accountID := result.TenantAccountID.String()
	if result.TenantAccountID == uuid.Nil {
		accountID = result.AccountID
	}
	trace := logging.Fields(
		"task_id", result.TargetID.String(),
		// The worker task_id IS campaign_targets.id.
		"target_id", result.TargetID.String(),
		"campaign_id", result.CampaignID.String(),
		"tenant_id", result.TenantID.String(),
		"tenant_account_id", accountID,
		"messenger_type", result.MessengerType,
		"chat_id", result.ChatID,
		"status", string(result.Status),
		// `status` is the delivery result reported by the worker.
		"delivery_result", string(result.Status),
		"error_code", result.ErrorCode,
		"error_message", logging.StrDeref(result.ErrorMessage),
		"user_not_found_by_phone", result.Status == domain.TaskStatusUserNotFoundByPhone,
		"reply_received", result.ReplyText != nil,
	)
	// Elapsed time is only meaningful when the worker stamped the result.
	if !result.Timestamp.IsZero() {
		ageMS := time.Since(result.Timestamp).Milliseconds()
		if ageMS < 0 {
			ageMS = 0
		}
		trace["worker_timestamp"] = result.Timestamp.UTC().Format(time.RFC3339Nano)
		trace["result_age_ms"] = ageMS
	}
	return trace
}

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
	cfg       config.Config
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

func NewResultConsumer(repo domain.CampaignRepository, uc *usecase.CampaignUseCase, amqpConn *amqp091.Connection, queueName string, blocklist BlocklistUpdater, cfg config.Config) (*ResultConsumer, error) {
	rc := &ResultConsumer{
		repo:      repo,
		uc:        uc,
		amqpConn:  amqpConn,
		queueName: queueName,
		blocklist: blocklist,
		cfg:       cfg,
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
		logging.Error("RABBITMQ_CHANNEL_OPEN_FAILED", logging.Fields(
			"component", "result_consumer", "queue", rc.queueName, "error", err.Error(),
		))
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
		logging.Error("RABBITMQ_QUEUE_DECLARE_FAILED", logging.Fields(
			"component", "result_consumer", "queue", rc.queueName, "error", err.Error(),
		))
		return err
	}

	rc.amqpChan = ch
	log.Println("ResultConsumer: reconnected to RabbitMQ channel")
	logging.Info("RABBITMQ_RECONNECTED", logging.Fields(
		"component", "result_consumer", "queue", rc.queueName,
	))
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
		logging.Error("RABBITMQ_CHANNEL_OPEN_FAILED", logging.Fields(
			"component", "result_consumer", "queue", rc.queueName, "error", err.Error(),
		))
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
		logging.Error("RABBITMQ_QUEUE_DECLARE_FAILED", logging.Fields(
			"component", "result_consumer", "queue", rc.queueName, "error", err.Error(),
		))
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
		logging.Error("RABBITMQ_CONSUME_FAILED", logging.Fields(
			"component", "result_consumer", "queue", rc.queueName, "consumer_tag", "result-consumer", "error", err.Error(),
		))
		return err
	}

	log.Println("ResultConsumer: started consuming messages")
	logging.Info("RABBITMQ_CONSUMER_STARTED", logging.Fields(
		"component", "result_consumer", "queue", rc.queueName, "consumer_tag", "result-consumer",
	))

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

	// Track replies together with their account provenance; they are grouped by
	// (tenant_id, tenant_account_id) once the batch has been processed.
	var observedReplies []observedReply

	for _, item := range toProcess {
		result := item.result
		// Diagnostic: a worker result arrived at the orchestrator. Emitted
		// before chat_id mapping resolution so it reflects exactly what the
		// worker sent; resolved identifiers follow in TARGET_STATUS_UPDATE_*.
		trace := resultTrace(&result)
		logging.Info("WORKER_RESULT_RECEIVED", trace)

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
				// Admin notification result: TargetID/CampaignID are empty, but we know
				// the tenant, the phone number and (reported by the worker) the sender
				// account. Persist the chat_id in admin_chat_phone_mappings — a table
				// separate from campaign/customer chat_phone_mappings — against the
				// account that actually sent it.
				adminAccountID := resultAccountID(result)
				if adminAccountID == uuid.Nil {
					// Without the sender account the mapping would be ambiguous: never
					// guess an account for an admin chat.
					log.Printf("ResultConsumer: admin chat mapping not saved (sender account unknown): tenant=%s phone=%s chat_id=%s", result.TenantID, result.PhoneNumber, result.ChatID)
					logging.Error("ADMIN_MAPPING_ACCOUNT_UNKNOWN", logging.WithFields(trace,
						"reason", "tenant_account_id_unknown",
						"chat_id", result.ChatID))
				} else if err := rc.repo.UpsertAdminChatMapping(processCtx, result.ChatID, result.TenantID, adminAccountID, result.PhoneNumber, result.MessengerType); err != nil {
					log.Printf("ResultConsumer: failed to upsert admin chat mapping for chat_id=%s phone=%s tenant_account_id=%s: %v", result.ChatID, result.PhoneNumber, adminAccountID, err)
					logging.Error("ADMIN_MAPPING_SAVE_FAILED", logging.WithFields(trace,
						"error", err.Error(),
						"chat_id", result.ChatID))
				} else {
					log.Printf("ResultConsumer: admin chat mapping saved: chat_id=%s tenant=%s tenant_account_id=%s phone=%s", result.ChatID, result.TenantID, adminAccountID, result.PhoneNumber)
					logging.Info("ADMIN_MAPPING_SAVED", logging.WithFields(trace,
						"chat_id", result.ChatID))
				}
				// Admin chats are never resolved through chat_phone_mappings and there
				// is no campaign target to transition: the result is fully handled here.
				cancel()
				if err := item.msg.Ack(false); err != nil {
					log.Printf("ResultConsumer: failed to ack message: %v", err)
				}
				continue
			}

			if result.TargetID == uuid.Nil || result.CampaignID == uuid.Nil || strings.TrimSpace(result.PhoneNumber) == "" {
				mapping, err := rc.repo.GetChatPhoneMappingByChatID(processCtx, result.ChatID)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						log.Printf("ResultConsumer: chat_id mapping not found (skipping): chat_id=%s status=%s", result.ChatID, result.Status)
						logging.Warn("WORKER_RESULT_REJECTED", logging.WithFields(trace,
							"reason", "chat_id_mapping_not_found"))
					} else {
						log.Printf("ResultConsumer: failed to resolve chat_id mapping (skipping): chat_id=%s err=%v", result.ChatID, err)
						logging.Warn("WORKER_RESULT_REJECTED", logging.WithFields(trace,
							"reason", "chat_id_mapping_lookup_failed",
							"error", err.Error()))
					}
					cancel()
					_ = item.msg.Ack(false)
					continue
				}
				result.TargetID = mapping.CampaignTargetID
				result.CampaignID = mapping.CampaignID
				result.PhoneNumber = mapping.PhoneNormalized
				if result.TenantAccountID == uuid.Nil && mapping.TenantAccountID != uuid.Nil {
					result.TenantAccountID = mapping.TenantAccountID
				}
				if mapping.CampaignTargetID == uuid.Nil {
					log.Printf("ResultConsumer: chat_id=%s maps to admin/non-campaign row (skipping status=%s)", result.ChatID, result.Status)
					logging.Info("WORKER_RESULT_REJECTED", logging.WithFields(trace,
						"reason", "admin_or_non_campaign_row",
						"resolved_campaign_id", result.CampaignID.String()))
					cancel()
					_ = item.msg.Ack(false)
					continue
				}
			}
		}

		if result.Status == domain.TaskStatusSent && result.TenantID != uuid.Nil && result.TargetID == uuid.Nil && result.CampaignID == uuid.Nil {
			// Admin-notification send result: recorded as a chat mapping above,
			// there is no campaign target to transition.
			logging.Info("WORKER_RESULT_REJECTED", logging.WithFields(trace,
				"reason", "admin_notification_result"))
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
			logging.Warn("WORKER_RESULT_REJECTED", logging.WithFields(trace,
				"reason", "unresolved_target_mapping"))
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
							logging.Info("WORKER_RESULT_REJECTED", logging.WithFields(trace,
								"reason", "duplicate_reply"))
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

			// Preserve the sender-account provenance of the reply: only the account
			// that observed it may generate (and send) the notification.
			observedReplies = append(observedReplies, observedReply{
				TenantID:        campaign.TenantID,
				TenantAccountID: replyAccountID(result.TenantAccountID, target),
				Reply: domain.ClientReplyInfo{
					UserPhone: target.PhoneNormalized,
					UserName:  target.ClientName,
					Message:   *result.ReplyText,
					Time:      result.Timestamp,
				},
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
			errMsg := result.ErrorMessage
			if errMsg == nil {
				msg := "user not found by phone"
				errMsg = &msg
			}
			_, err := rc.repo.UpdateTargetStatus(processCtx, result.TargetID, domain.TaskStatusUserNotFoundByPhone, errMsg, nil)
			if err != nil {
				log.Printf("ResultConsumer: failed to update target %s status to %s: %v", result.TargetID, result.Status, err)
				processedOK = false
			}
		} else if isSendFailureResult(result) {
			if err := rc.handleSendFailure(processCtx, result); err != nil {
				log.Printf("ResultConsumer: failed to handle send failure for target %s: %v", result.TargetID, err)
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

	// Group replies by (tenant_id, tenant_account_id): every account produces its
	// own admin notification, published to its own account-specific queue.
	tenantReplies := groupRepliesByTenantAccount(observedReplies)

	// Now, send tenant admin notifications
	for group, replies := range tenantReplies {
		if len(replies) == 0 {
			continue
		}

		tenantID := group.TenantID
		tenantAccountID := group.TenantAccountID
		if tenantAccountID == uuid.Nil {
			// Never send an account-ambiguous admin notification: without the sender
			// account it cannot be routed, and the resulting chat mapping could not
			// be scoped to an account.
			log.Printf("ResultConsumer: skipping admin notification for tenant %s (%d replies): sender account unknown", tenantID, len(replies))
			logging.Warn("ADMIN_NOTIFY_SKIPPED", logging.Fields(
				"tenant_id", tenantID.String(),
				"reason", "tenant_account_id_unknown",
				"reply_count", len(replies),
			))
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
				// Admin mappings are looked up per sender account: the same admin phone
				// may be mapped differently (or not at all) for each account, and a
				// campaign mapping with the same phone must never be picked up here.
				if mapping, err := rc.repo.GetAdminChatPhoneMappingByPhone(tenantCtx, tenantID, tenantAccountID, adminNormalized, mt); err == nil && mapping != nil && mapping.ChatID != "" {
					adminChatID = mapping.ChatID
					adminUseChatID = true
					log.Printf("ResultConsumer: admin chat_id found in admin mappings: chat_id=%s tenant=%s tenant_account_id=%s phone=%s", adminChatID, tenantID, tenantAccountID, adminPhone)
				} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					log.Printf("ResultConsumer: admin chat_id lookup failed (fallback to phone) tenant=%s tenant_account_id=%s phone=%s: %v", tenantID, tenantAccountID, adminPhone, err)
				}
			}

			// Prepare the task for this specific phone number
			task := domain.TenantAdminNotificationTask{
				TenantID:        tenantID.String(),
				TenantAccountID: tenantAccountID.String(),
				TenantPhone:     adminPhone,
				ChatID:          adminChatID,
				UseChatID:       adminUseChatID,
				Replies:         replies,
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

			// Publish to this account's OWN notification queue: the notification must
			// be sent by the MAX account that observed the replies.
			notifyQueue := TenantAdminNotifyQueue(tenantAccountID.String())
			if err := rc.ensureNotifyQueue(notifyQueue); err != nil {
				log.Printf("ResultConsumer: failed to declare notify queue %s: %v", notifyQueue, err)
				continue
			}

			// --- Diagnostic: tenant admin notification publish (logging only) ---
			// The notification is a separate worker task published to the
			// account-specific queue
			// (tasks.messages.tenant_admin_notify.account.<tenant_account_id>), so it
			// is traced separately from the campaign send tasks.
			notifyTrace := logging.Fields(
				"tenant_id", tenantID.String(),
				"tenant_account_id", tenantAccountID.String(),
				"phone", logging.MaskPhone(adminPhone),
				"messenger_type", string(domain.DefaultMessengerType),
				"chat_id", adminChatID,
				"use_chat_id", adminUseChatID,
				"reply_count", len(replies),
				"queue", notifyQueue,
			)
			logging.Info("ADMIN_NOTIFY_PUBLISH_START", notifyTrace)
			notifyStartedAt := time.Now()

			err = rc.amqpChan.PublishWithContext(
				publishCtx,
				"",          // exchange
				notifyQueue, // routing key (default exchange: == queue name)
				false,       // mandatory
				false,       // immediate
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
						notifyQueue,
						false,
						false,
						amqp091.Publishing{
							ContentType: "application/json",
							Body:        payload,
						},
					)
					if err != nil {
						log.Printf("ResultConsumer: still failed to publish notification for tenant %s phone %s: %v", tenantID, adminPhone, err)
						logging.Error("ADMIN_NOTIFY_PUBLISH_FAILED", logging.WithFields(notifyTrace,
							"error", err.Error(),
							"retried_after_reconnect", true,
							"publish_duration_ms", time.Since(notifyStartedAt).Milliseconds()))
					} else {
						log.Printf("ResultConsumer: published notification to tenant %s tenant_account_id %s phone %s (%d replies, chat_id=%s, use_chat_id=%v, queue=%s)", tenantID, tenantAccountID, adminPhone, len(replies), adminChatID, adminUseChatID, notifyQueue)
						logging.Info("ADMIN_NOTIFY_PUBLISHED", logging.WithFields(notifyTrace,
							"retried_after_reconnect", true,
							"publish_duration_ms", time.Since(notifyStartedAt).Milliseconds()))
					}
				} else {
					log.Printf("ResultConsumer: failed to reconnect for tenant %s phone %s: %v", tenantID, adminPhone, err)
					logging.Error("ADMIN_NOTIFY_PUBLISH_FAILED", logging.WithFields(notifyTrace,
						"error", err.Error(),
						"reconnect_failed", true,
						"publish_duration_ms", time.Since(notifyStartedAt).Milliseconds()))
				}
			} else {
				log.Printf("ResultConsumer: published notification to tenant %s tenant_account_id %s phone %s (%d replies, chat_id=%s, use_chat_id=%v, queue=%s)", tenantID, tenantAccountID, adminPhone, len(replies), adminChatID, adminUseChatID, notifyQueue)
				logging.Info("ADMIN_NOTIFY_PUBLISHED", logging.WithFields(notifyTrace,
					"publish_duration_ms", time.Since(notifyStartedAt).Milliseconds()))
			}
		}
	}
}

func (rc *ResultConsumer) ensureNotifyQueue(queueName string) error {
	if rc.amqpChan == nil {
		return fmt.Errorf("amqp channel is nil")
	}
	if _, err := rc.amqpChan.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		logging.Error("RABBITMQ_QUEUE_DECLARE_FAILED", logging.Fields(
			"component", "result_consumer", "queue", queueName, "error", err.Error(),
		))
		return err
	}
	return nil
}

func isSendFailureResult(result domain.TargetResult) bool {
	switch result.Status {
	case domain.TaskStatusFailed, domain.TaskStatusDeliveryUnknown, domain.TaskStatusRetryPending:
		return true
	}
	if result.ErrorCode == "" {
		return false
	}
	switch domain.ClassifyErrorCode(result.ErrorCode) {
	case domain.DispositionRetryable, domain.DispositionUnknown:
		return result.Status != domain.TaskStatusSent &&
			result.Status != domain.TaskStatusDelivered &&
			result.Status != domain.TaskStatusViewed &&
			result.Status != domain.TaskStatusReplied
	default:
		return false
	}
}

func (rc *ResultConsumer) handleSendFailure(ctx context.Context, result domain.TargetResult) error {
	errorCode := strings.TrimSpace(result.ErrorCode)
	errMsg := ""
	if result.ErrorMessage != nil {
		errMsg = *result.ErrorMessage
	}

	if errorCode == "" {
		if result.Status == domain.TaskStatusDeliveryUnknown {
			errorCode = domain.ErrorCodeDeliveryUnknown
		} else {
			// Legacy failed results without structured error_code stay terminal.
			_, err := rc.repo.UpdateTargetStatus(ctx, result.TargetID, domain.TaskStatusFailed, &errMsg, nil)
			return err
		}
	}

	accountID := result.TenantAccountID
	if accountID == uuid.Nil && result.AccountID != "" {
		if aid, err := uuid.Parse(result.AccountID); err == nil {
			accountID = aid
		}
	}

	switch domain.ClassifyErrorCode(errorCode) {
	case domain.DispositionUnknown:
		log.Printf("ResultConsumer: DELIVERY_UNKNOWN for target %s — no automatic retry", result.TargetID)
		return rc.repo.MarkTargetDeliveryUnknown(ctx, result.TargetID, errorCode, errMsg)
	case domain.DispositionRetryable:
		next := time.Now().UTC().Add(rc.cfg.RetryDelay())
		cooldownUntil := time.Time{}
		if domain.IsSessionHealthError(errorCode) && accountID != uuid.Nil {
			cooldownUntil = time.Now().UTC().Add(rc.cfg.AccountCooldown())
		}
		retried, err := rc.repo.ApplyRetryableFailure(ctx, result.TargetID, accountID, errorCode, errMsg, next, cooldownUntil, rc.cfg.MaxRetryAttempts)
		if err != nil {
			return err
		}
		if retried {
			log.Printf("ResultConsumer: target %s -> retry_pending code=%s next=%s account=%s", result.TargetID, errorCode, next.Format(time.RFC3339), accountID)
		} else {
			log.Printf("ResultConsumer: target %s exhausted retries or already terminal code=%s", result.TargetID, errorCode)
		}
		return nil
	default:
		_, err := rc.repo.UpdateTargetStatus(ctx, result.TargetID, domain.TaskStatusFailed, &errMsg, nil)
		return err
	}
}
