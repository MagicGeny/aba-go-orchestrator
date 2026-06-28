package worker

import (
	"context"
	"log"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/rabbitmq/amqp091-go"
)

type OutboxWorker struct {
	repo    domain.OutboxRepository
	amqpConn *amqp091.Connection
	amqpChan *amqp091.Channel
	queueName string
}

func NewOutboxWorker(repo domain.OutboxRepository, amqpConn *amqp091.Connection, queueName string) (*OutboxWorker, error) {
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &OutboxWorker{
		repo:      repo,
		amqpConn:  amqpConn,
		amqpChan:  ch,
		queueName: queueName,
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

	for _, msg := range messages {
		err := w.amqpChan.PublishWithContext(ctx,
			"",          // exchange
			w.queueName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        msg.Payload,
			},
		)
		if err != nil {
			log.Printf("failed to publish message %s: %v", msg.ID, err)
			continue
		}

		if err := w.repo.MarkAsProcessed(ctx, msg.ID); err != nil {
			log.Printf("failed to mark message %s as processed: %v", msg.ID, err)
		}
	}
}
