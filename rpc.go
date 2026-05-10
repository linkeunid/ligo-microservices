package ligo_microservices

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Handle[T, R any](b *Broker, pattern string, handler func(ctx context.Context, input T) (R, error)) {
	entry := handlerEntry{
		invoke: func(ctx context.Context, data []byte, codec Codec) (any, error) {
			var input T
			if err := codec.Decode(data, &input); err != nil {
				return nil, fmt.Errorf("microservices: decode %s: %w", pattern, err)
			}
			return handler(ctx, input)
		},
	}
	registerHandler(b, pattern, entry)
	b.bindPattern(pattern)
}

func Send[T any](ctx context.Context, b *Broker, pattern string, payload any) (T, error) {
	var zero T
	if b.ch == nil {
		return zero, ErrNotConnected
	}

	data, err := b.cfg.Codec.Encode(payload)
	if err != nil {
		return zero, fmt.Errorf("microservices: encode: %w", err)
	}

	id := newID()
	env := envelope{Pattern: pattern, Data: data, ID: id}
	body, err := json.Marshal(env)
	if err != nil {
		return zero, fmt.Errorf("microservices: marshal: %w", err)
	}

	ch := make(chan *response, 1)
	b.mu.Lock()
	b.pending[id] = ch
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
	}()

	if err := b.ch.PublishWithContext(ctx, b.cfg.Exchange, pattern, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: id,
		ReplyTo:       "amq.rabbitmq.reply-to",
		Body:          body,
	}); err != nil {
		return zero, fmt.Errorf("microservices: publish: %w", err)
	}

	timeout := b.cfg.Timeout
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case resp := <-ch:
		if resp.Err != "" {
			return zero, &BrokerError{Type: resp.ErrType, Message: resp.Err}
		}
		var result T
		if err := b.cfg.Codec.Decode(resp.Data, &result); err != nil {
			return zero, fmt.Errorf("microservices: decode response: %w", err)
		}
		return result, nil
	case <-deadlineCtx.Done():
		return zero, fmt.Errorf("microservices: timeout waiting for %s: %w", pattern, deadlineCtx.Err())
	}
}
