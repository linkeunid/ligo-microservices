package ligo_microservices

import (
	"context"
	"errors"
	"testing"

	"github.com/linkeunid/ligo"
)

func TestNewBroker(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	if b == nil {
		t.Fatal("NewBroker returned nil")
	}
}

func TestBrokerImplementsRegisterable(t *testing.T) {
	var _ ligo.Registerable = &Broker{}
}

func TestHandlePanicsOnDuplicate(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)
	b.handlers["orders.create"] = handlerEntry{}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate handler")
		}
	}()
	registerHandler(b, "orders.create", handlerEntry{})
}

func TestHandleRegistersEntry(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	entry := handlerEntry{
		invoke: func(ctx context.Context, data []byte, codec Codec) (any, error) {
			return "hello", nil
		},
	}
	registerHandler(b, "test.pattern", entry)

	if len(b.handlers) != 1 {
		t.Fatalf("handlers: got %d, want 1", len(b.handlers))
	}
	if _, ok := b.handlers["test.pattern"]; !ok {
		t.Fatal("test.pattern not in handlers")
	}
}

func TestMessageFromEnvelope(t *testing.T) {
	env := envelope{Pattern: "test.echo", Data: []byte(`{"msg":"hi"}`), ID: "abc123"}
	msg := newMessage(env)
	if msg.Pattern != "test.echo" {
		t.Errorf("pattern: got %q, want %q", msg.Pattern, "test.echo")
	}
	if msg.ID != "abc123" {
		t.Errorf("id: got %q, want %q", msg.ID, "abc123")
	}
	if string(msg.Data) != `{"msg":"hi"}` {
		t.Errorf("data: got %q, want %q", string(msg.Data), `{"msg":"hi"}`)
	}
}

func TestRegisterEventHandler(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.eventHandlers = make(map[string]handlerEntry)

	entry := handlerEntry{
		invoke: func(ctx context.Context, data []byte, codec Codec) (any, error) {
			return nil, nil
		},
	}
	registerEventHandler(b, "user.created", entry)

	if len(b.eventHandlers) != 1 {
		t.Fatalf("eventHandlers: got %d, want 1", len(b.eventHandlers))
	}
	if _, ok := b.eventHandlers["user.created"]; !ok {
		t.Fatal("user.created not in eventHandlers")
	}
}

func TestRegisterEventHandlerPanicsOnDuplicate(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.eventHandlers = make(map[string]handlerEntry)
	b.eventHandlers["user.created"] = handlerEntry{}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate event handler")
		}
	}()
	registerEventHandler(b, "user.created", handlerEntry{})
}

func TestSendReturnsErrorWhenNotConnected(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	_, err := Send[string](context.Background(), b, "test", nil)
	if err == nil {
		t.Fatal("expected error when broker not connected")
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("error: got %v, want ErrNotConnected", err)
	}
}
