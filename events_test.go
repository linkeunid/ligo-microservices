package ligo_microservices

import (
	"context"
	"testing"
)

func TestEmitReturnsErrorWhenNotConnected(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	err := Emit(context.Background(), b, "test.event", map[string]string{"key": "value"})
	if err == nil {
		t.Fatal("expected error when broker not connected")
	}
}

func TestOnRegistersEventHandler(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})

	called := false
	On[map[string]string](b, "user.created", func(ctx context.Context, input map[string]string) error {
		called = true
		return nil
	})

	_ = called // handler closure captures the variable

	if len(b.eventHandlers) != 1 {
		t.Fatalf("eventHandlers: got %d, want 1", len(b.eventHandlers))
	}
}
