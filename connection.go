package ligo_microservices

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) connect() error {
	// Tear down any previous lifecycle. cancel() makes the old consumer
	// and watcher goroutines exit; pending RPC callers see their reply
	// channel close and surface ErrConnectionLost.
	b.chMu.Lock()
	oldCancel := b.cancel
	oldCh := b.ch
	oldConn := b.conn
	b.cancel = nil
	b.ch = nil
	b.conn = nil
	b.chMu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	if oldCh != nil {
		_ = oldCh.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
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

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("microservices: channel: %w", err)
	}

	if exchErr := ch.ExchangeDeclare(b.cfg.Exchange, "topic", true, false, false, false, nil); exchErr != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("microservices: exchange declare: %w", exchErr)
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
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("microservices: queue declare: %w", err)
	}

	// Publish new state under chMu so concurrent Send/bindPattern observe
	// a coherent (conn, ch, handlerQueue) triple.
	ctx, cancel := context.WithCancel(context.Background())
	b.chMu.Lock()
	b.conn = conn
	b.ch = ch
	b.handlerQueue = queue.Name
	b.cancel = cancel
	b.chMu.Unlock()

	go b.consumeReplies(ctx)
	go b.consumeHandlers(ctx)
	// Arm the watcher last so it only fires once the consumers are up.
	go b.watchConnection(ctx)

	return nil
}

func (b *Broker) disconnect() error {
	// Flip the closed flag first so the watcher goroutine — which may
	// fire mid-disconnect when amqp.Connection.Close triggers
	// NotifyClose — sees it and skips the reconnect path.
	b.closed.Store(true)

	b.chMu.Lock()
	cancel := b.cancel
	ch := b.ch
	conn := b.conn
	b.cancel = nil
	b.ch = nil
	b.conn = nil
	b.chMu.Unlock()

	if cancel != nil {
		cancel()
	}
	var err error
	if ch != nil {
		err = ch.Close()
	}
	if conn != nil {
		if e := conn.Close(); err == nil {
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
	b.chMu.Lock()
	if b.ch != nil {
		_ = b.ch.PublishWithContext(context.Background(), "", replyTo, false, false, amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationID,
			Body:          body,
		})
	}
	b.chMu.Unlock()
}

func (b *Broker) sendErrorResponse(id, replyTo, correlationID, errType, errMsg string) {
	if replyTo == "" {
		return
	}
	resp := response{ID: id, Err: errMsg, ErrType: errType}
	b.sendResponse(resp, replyTo, correlationID)
}
