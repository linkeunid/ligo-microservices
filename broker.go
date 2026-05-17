package ligo_microservices

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkeunid/ligo"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrNotConnected is returned by Send when the broker has not yet
	// connected (or has been shut down).
	ErrNotConnected = errors.New("microservices: broker not connected")
	// ErrConnectionLost is returned to in-flight Send callers when the
	// broker's underlying AMQP connection drops mid-request. The reply
	// channel is drained as part of reconnect, so the caller gets a
	// determinate error instead of timing out.
	ErrConnectionLost = errors.New("microservices: connection lost during request")
)

type Broker struct {
	cfg           RabbitMQConfig
	conn          *amqp.Connection
	ch            *amqp.Channel
	handlers      map[string]handlerEntry
	eventHandlers map[string]handlerEntry
	pending       map[string]chan *response
	handlerQueue  string
	mu            sync.Mutex
	// chMu serializes writes on b.ch. amqp091 channels are not safe for
	// concurrent use: Publish, QueueBind, Consume setup, etc. each send
	// frames and races corrupt the channel state (RabbitMQ closes it with
	// 503 "unexpected command received"). Acquire chMu around every method
	// call on b.ch — consumer goroutines only read from the deliveries
	// channel returned by Consume, so they do not need to hold it.
	chMu sync.Mutex
	// closed is flipped to true by disconnect() and consulted by the
	// reconnect goroutine to suppress retries on user-initiated shutdown.
	closed atomic.Bool
	cancel context.CancelFunc
}

func NewBroker(cfg RabbitMQConfig) *Broker {
	if cfg.Codec == nil {
		cfg.Codec = JSONCodec
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Broker{
		cfg:           cfg,
		handlers:      make(map[string]handlerEntry),
		eventHandlers: make(map[string]handlerEntry),
		pending:       make(map[string]chan *response),
	}
}

func (b *Broker) Register(r *ligo.HookRegistry) {
	r.OnInit(b.connect)
	r.OnShutdown(b.disconnect)
}
