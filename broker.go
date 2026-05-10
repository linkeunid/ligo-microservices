package ligo_microservices

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/linkeunid/ligo"
)

var ErrNotConnected = errors.New("microservices: broker not connected")

type Broker struct {
	cfg           RabbitMQConfig
	conn          *amqp.Connection
	ch            *amqp.Channel
	handlers      map[string]handlerEntry
	eventHandlers map[string]handlerEntry
	pending       map[string]chan *response
	handlerQueue  string
	mu            sync.Mutex
	cancel        context.CancelFunc
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
