package ligo_microservices

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) connect() error {
	if b.cancel != nil {
		b.cancel()
	}

	b.mu.Lock()
	for id, ch := range b.pending {
		close(ch)
		delete(b.pending, id)
	}
	b.mu.Unlock()

	conn, err := amqp.Dial(b.cfg.URL)
	if err != nil {
		return fmt.Errorf("microservices: dial: %w", err)
	}
	b.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("microservices: channel: %w", err)
	}
	b.ch = ch

	if err := ch.ExchangeDeclare(b.cfg.Exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("microservices: exchange declare: %w", err)
	}

	// Named queue → durable, non-exclusive, non-auto-delete (survives
	// restarts, sharable across consumers). Empty name → server-generated,
	// exclusive, auto-delete, non-durable (ephemeral per-process).
	var (
		qName      = b.cfg.Queue
		durable    = qName != ""
		exclusive  = qName == ""
		autoDelete = qName == ""
	)
	queue, err := ch.QueueDeclare(qName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return fmt.Errorf("microservices: queue declare: %w", err)
	}
	b.handlerQueue = queue.Name

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel

	go b.consumeReplies(ctx)
	go b.consumeHandlers(ctx)

	return nil
}

func (b *Broker) disconnect() error {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	var err error
	if b.ch != nil {
		err = b.ch.Close()
		b.ch = nil
	}
	if b.conn != nil {
		e := b.conn.Close()
		b.conn = nil
		if err == nil {
			err = e
		}
	}
	return err
}

func (b *Broker) sendResponse(resp response, replyTo, correlationID string) {
	body, err := json.Marshal(resp)
	if err != nil {
		return
	}
	b.ch.PublishWithContext(context.Background(), "", replyTo, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		Body:          body,
	})
}

func (b *Broker) sendErrorResponse(id, replyTo, correlationID, errType, errMsg string) {
	if replyTo == "" {
		return
	}
	resp := response{ID: id, Err: errMsg, ErrType: errType}
	b.sendResponse(resp, replyTo, correlationID)
}
