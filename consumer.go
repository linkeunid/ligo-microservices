package ligo_microservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) consumeReplies(ctx context.Context) {
	deliveries, err := b.ch.Consume("amq.rabbitmq.reply-to", "", true, false, false, false, nil)
	if err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}
			var resp response
			if err := json.Unmarshal(msg.Body, &resp); err != nil {
				continue
			}
			b.mu.Lock()
			if ch, exists := b.pending[resp.ID]; exists {
				ch <- &resp
				delete(b.pending, resp.ID)
			}
			b.mu.Unlock()
		}
	}
}

func (b *Broker) consumeHandlers(ctx context.Context) {
	deliveries, err := b.ch.Consume(b.handlerQueue, "", false, false, false, false, nil)
	if err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}
			b.handleMessage(msg)
		}
	}
}

func (b *Broker) handleMessage(msg amqp.Delivery) {
	var env envelope
	if err := json.Unmarshal(msg.Body, &env); err != nil {
		msg.Nack(false, false)
		return
	}

	b.mu.Lock()
	rpcEntry, hasRPC := b.handlers[env.Pattern]
	eventEntry, hasEvent := b.eventHandlers[env.Pattern]
	b.mu.Unlock()

	if !hasRPC && !hasEvent {
		b.sendErrorResponse(env.ID, msg.ReplyTo, msg.CorrelationId,
			"NO_HANDLER", fmt.Sprintf("no handler for %q", env.Pattern))
		msg.Ack(false)
		return
	}

	ctx := context.Background()

	if hasRPC {
		result, err := rpcEntry.invoke(ctx, env.Data, b.cfg.Codec)

		resp := response{ID: env.ID}
		if err != nil {
			var brokerErr *BrokerError
			if errors.As(err, &brokerErr) {
				resp.Err = brokerErr.Message
				resp.ErrType = brokerErr.Type
			} else {
				resp.Err = err.Error()
				resp.ErrType = "Internal"
			}
		} else {
			data, encErr := b.cfg.Codec.Encode(result)
			if encErr != nil {
				resp.Err = encErr.Error()
				resp.ErrType = "Internal"
			} else {
				resp.Data = data
			}
		}

		if msg.ReplyTo != "" {
			b.sendResponse(resp, msg.ReplyTo, msg.CorrelationId)
		}
		msg.Ack(false)
		return
	}

	_, err := eventEntry.invoke(ctx, env.Data, b.cfg.Codec)
	if err != nil {
		msg.Nack(false, false)
		return
	}
	msg.Ack(false)
}
