package ligo_microservices

import (
	"context"
	"fmt"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RetryConfig struct {
	MaxAttempts int
	Delay       time.Duration
	MaxDelay    time.Duration
	OnRetry     func(attempt int, err error)
	OnReconnect func()
}

func (c *RetryConfig) applyDefaults() {
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.Delay == 0 {
		c.Delay = time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 30 * time.Second
	}
}

func (c *RetryConfig) backoff(attempt int) time.Duration {
	d := time.Duration(float64(c.Delay) * math.Pow(2, float64(attempt)))
	if d > c.MaxDelay {
		return c.MaxDelay
	}
	return d
}

func (b *Broker) watchConnection(ctx context.Context) {
	closeCh := b.conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeCh:
			if !ok {
				return
			}
			b.reconnect(ctx, amqpErr)
			return
		}
	}
}

func (b *Broker) reconnect(ctx context.Context, amqpErr *amqp.Error) {
	cfg := b.cfg.Retry
	cfg.applyDefaults()

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, amqpErr)
		}

		time.Sleep(cfg.backoff(attempt))

		if err := b.connect(); err != nil {
			continue
		}

		if err := b.rebindAll(); err != nil {
			continue
		}

		if cfg.OnReconnect != nil {
			cfg.OnReconnect()
		}

		// Watch the new connection for future disconnects.
		go b.watchConnection(ctx)
		return
	}
}

func (b *Broker) rebindAll() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for pattern := range b.handlers {
		if err := b.ch.QueueBind(b.handlerQueue, pattern, b.cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("microservices: rebind %s: %w", pattern, err)
		}
	}
	for pattern := range b.eventHandlers {
		if err := b.ch.QueueBind(b.handlerQueue, pattern, b.cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("microservices: rebind event %s: %w", pattern, err)
		}
	}
	return nil
}
