package worker

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/rabbitmq/amqp091-go"
)

type ReplyPoller struct {
	repo       domain.CampaignRepository
	amqpConn   *amqp091.Connection
	amqpChan   *amqp091.Channel
	queueName  string
	mu         sync.Mutex
	isRunning  bool
}

func NewReplyPoller(repo domain.CampaignRepository, amqpConn *amqp091.Connection, queueName string) (*ReplyPoller, error) {
	p := &ReplyPoller{
		repo:      repo,
		amqpConn:  amqpConn,
		queueName: queueName,
	}
	err := p.reconnect()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *ReplyPoller) reconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close old channel if exists
	if p.amqpChan != nil {
		_ = p.amqpChan.Close()
	}

	// Create new channel
	ch, err := p.amqpConn.Channel()
	if err != nil {
		return err
	}

	// Declare queue (idempotent)
	_, err = ch.QueueDeclare(
		p.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	p.amqpChan = ch
	log.Println("ReplyPoller: Reconnected to RabbitMQ channel")
	return nil
}

func (p *ReplyPoller) Run(ctx context.Context) {
	// Poll every 10 minutes as requested
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	// Initial poll
	//p.PollActiveCampaigns(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.PollActiveCampaigns(ctx)
		}
	}
}

func (p *ReplyPoller) PollActiveCampaigns(_ context.Context) {
	// Create independent context with timeout to avoid context canceled errors
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	p.mu.Lock()
	if p.isRunning {
		log.Println("ReplyPoller: Already running, skipping...")
		p.mu.Unlock()
		return
	}
	p.isRunning = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.isRunning = false
		p.mu.Unlock()
	}()

	log.Println("ReplyPoller: Checking for active campaigns to poll...")

	// Get campaigns in processing status only
	processingCampaigns, err := p.repo.GetCampaignsByStatus(ctx, domain.CampaignStatusProcessing)
	if err != nil {
		log.Printf("ReplyPoller: failed to get processing campaigns: %v", err)
		return
	}

	for _, c := range processingCampaigns {
		// Check if 24 hours have passed since campaign creation
		if time.Since(c.CreatedAt) > 24*time.Hour {
			log.Printf("ReplyPoller: Campaign %s has been running for 24h, marking as completed", c.ID)
			err := p.repo.UpdateCampaignStatus(ctx, c.ID, domain.CampaignStatusCompleted)
			if err != nil {
				log.Printf("ReplyPoller: failed to mark campaign %s as completed: %v", c.ID, err)
			}
			continue
		}

		// Get targets that haven't replied yet (status is pending, sent, or delivered but not replied)
		targets, err := p.repo.GetCampaignTargetsWithStatus(ctx, c.ID, []domain.TaskStatus{
			domain.TaskStatusPending,
			domain.TaskStatusSent,
			domain.TaskStatusDelivered,
		})
		if err != nil {
			log.Printf("ReplyPoller: failed to get targets for campaign %s: %v", c.ID, err)
			continue
		}

		if len(targets) == 0 {
			continue
		}

		// Chunk targets into groups of max 5
		type ChunkTarget struct {
			TargetID        uuid.UUID `json:"target_id"`
			PhoneNormalized string    `json:"phone_normalized"`
		}
		var chunks [][]ChunkTarget
		chunkSize := 5
		for i := 0; i < len(targets); i += chunkSize {
			end := i + chunkSize
			if end > len(targets) {
				end = len(targets)
			}
			var chunk []ChunkTarget
			for _, t := range targets[i:end] {
				chunk = append(chunk, ChunkTarget{
					TargetID:        t.ID,
					PhoneNormalized: t.PhoneNormalized,
				})
			}
			chunks = append(chunks, chunk)
		}

		for i, chunk := range chunks {
			payload := map[string]any{
				"campaign_id": c.ID,
				"targets":     chunk,
			}
			body, _ := json.Marshal(payload)

			// Try publishing, reconnect once if failed
			err = p.amqpChan.PublishWithContext(ctx,
				"",          // exchange
				p.queueName, // routing key
				false,       // mandatory
				false,       // immediate
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        body,
				},
			)
			if err != nil {
				log.Printf("ReplyPoller: publish failed, trying to reconnect: %v", err)
				if reconnectErr := p.reconnect(); reconnectErr == nil {
					err = p.amqpChan.PublishWithContext(ctx,
						"",          // exchange
						p.queueName, // routing key
						false,       // mandatory
						false,       // immediate
						amqp091.Publishing{
							ContentType: "application/json",
							Body:        body,
						},
					)
				} else {
					log.Printf("ReplyPoller: failed to reconnect: %v", reconnectErr)
				}
			}
			if err != nil {
				log.Printf("ReplyPoller: failed to publish poll chunk %d for campaign %s: %v", i+1, c.ID, err)
			} else {
				log.Printf("ReplyPoller: published poll chunk %d for campaign %s with %d targets", i+1, c.ID, len(chunk))
			}
		}
	}
}
