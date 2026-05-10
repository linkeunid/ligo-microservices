package ligo_microservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/linkeunid/ligo"
)

var ErrNotConnected = errors.New("microservices: broker not connected")

type Broker struct {
	cfg          RabbitMQConfig
	conn         *amqp.Connection
	ch           *amqp.Channel
	handlers      map[string]handlerEntry
	eventHandlers map[string]handlerEntry
	pending       map[string]chan *response
	handlerQueue string
	mu           sync.Mutex
	cancel       context.CancelFunc
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

// Register implements ligo.Registerable for HookedFactory lifecycle.
func (b *Broker) Register(r *ligo.HookRegistry) {
	r.OnInit(b.connect)
	r.OnShutdown(b.disconnect)
}

func (b *Broker) connect() error {
	// Cancel previous goroutines from prior connection (reconnect path).
	if b.cancel != nil {
		b.cancel()
	}

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

	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
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

type handlerEntry struct {
	invoke func(ctx context.Context, data []byte, codec Codec) (any, error)
}

func registerTo(b *Broker, m map[string]handlerEntry, pattern, label string, entry handlerEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := m[pattern]; exists {
		panic(fmt.Sprintf("microservices: %s already registered for %q", label, pattern))
	}
	m[pattern] = entry
}

func registerHandler(b *Broker, pattern string, entry handlerEntry) {
	registerTo(b, b.handlers, pattern, "handler", entry)
}

func registerEventHandler(b *Broker, pattern string, entry handlerEntry) {
	registerTo(b, b.eventHandlers, pattern, "event handler", entry)
}

// bindPattern binds the handler queue to the exchange for a routing pattern.
// No-op if the broker is not connected.
func (b *Broker) bindPattern(pattern string) {
	if b.ch == nil {
		return
	}
	if err := b.ch.QueueBind(b.handlerQueue, pattern, b.cfg.Exchange, false, nil); err != nil {
		panic(fmt.Sprintf("microservices: bind %s: %v", pattern, err))
	}
}

// Handle registers a typed RPC handler for the given pattern.
// Panics if a handler is already registered for the same pattern.
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
