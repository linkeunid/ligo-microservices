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

// watchConnection blocks until either the parent ctx is canceled (user-
// initiated shutdown) or the AMQP server closes the connection. In the
// latter case it hands off to reconnect; it never restarts itself —
// the fresh watcher is armed at the end of a successful connect().
func (b *Broker) watchConnection(ctx context.Context) {
	b.chMu.Lock()
	conn := b.conn
	b.chMu.Unlock()
	if conn == nil {
		return
	}
	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-ctx.Done():
		return
	case amqpErr, ok := <-closeCh:
		if !ok || b.closed.Load() {
			// User-initiated shutdown — nothing to do.
			return
		}
		b.reconnect(ctx, amqpErr)
	}
}

// reconnect retries connect() with exponential backoff. Each successful
// connect() already arms a fresh watcher, so reconnect just returns
// after the first success.
func (b *Broker) reconnect(_ context.Context, amqpErr *amqp.Error) {
	cfg := b.cfg.Retry
	cfg.applyDefaults()

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if b.closed.Load() {
			return
		}
		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, amqpErr)
		}

		time.Sleep(cfg.backoff(attempt))
		if b.closed.Load() {
			return
		}

		if err := b.connect(); err != nil {
			continue
		}
		if err := b.rebindAll(); err != nil {
			// Channel up but binds failed. The next connect() will cancel
			// the current ctx and tear down conn/ch via its leading
			// cleanup; loop on to retry from scratch.
			continue
		}

		if cfg.OnReconnect != nil {
			cfg.OnReconnect()
		}
		return
	}
}

// rebindAll re-issues QueueBind for every registered RPC and event
// pattern. Called after a reconnect to restore routing to the (new)
// handler queue.
func (b *Broker) rebindAll() error {
	b.mu.Lock()
	patterns := make([]string, 0, len(b.handlers)+len(b.eventHandlers))
	for pattern := range b.handlers {
		patterns = append(patterns, pattern)
	}
	for pattern := range b.eventHandlers {
		patterns = append(patterns, pattern)
	}
	b.mu.Unlock()

	b.chMu.Lock()
	defer b.chMu.Unlock()
	if b.ch == nil {
		return fmt.Errorf("microservices: rebind: %w", ErrNotConnected)
	}
	for _, pattern := range patterns {
		if err := b.ch.QueueBind(b.handlerQueue, pattern, b.cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("microservices: rebind %s: %w", pattern, err)
		}
	}
	return nil
}
