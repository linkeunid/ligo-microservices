package ligo_microservices

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Emit(ctx context.Context, b *Broker, pattern string, payload any) error {
	if b.ch == nil {
		return ErrNotConnected
	}

	data, err := b.cfg.Codec.Encode(payload)
	if err != nil {
		return fmt.Errorf("microservices: encode: %w", err)
	}

	id := newID()
	env := envelope{Pattern: pattern, Data: data, ID: id}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("microservices: marshal: %w", err)
	}

	b.chMu.Lock()
	pubErr := b.ch.PublishWithContext(ctx, b.cfg.Exchange, pattern, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	b.chMu.Unlock()
	if pubErr != nil {
		return fmt.Errorf("microservices: publish: %w", pubErr)
	}

	return nil
}

// On registers a typed event handler for the given pattern.
// Panics if a handler is already registered for the same pattern.
func On[T any](b *Broker, pattern string, handler func(ctx context.Context, input T) error) {
	entry := handlerEntry{
		invoke: func(ctx context.Context, data []byte, codec Codec) (any, error) {
			var input T
			if err := codec.Decode(data, &input); err != nil {
				return nil, fmt.Errorf("microservices: decode %s: %w", pattern, err)
			}
			return nil, handler(ctx, input)
		},
	}
	registerEventHandler(b, pattern, entry)
	b.bindPattern(pattern)
}
