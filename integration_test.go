//go:build integration

package ligo_microservices

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRPCRoundTrip(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	type EchoInput struct {
		Message string `json:"message"`
	}
	type EchoOutput struct {
		Echo string `json:"echo"`
	}

	Handle[EchoInput, EchoOutput](broker, "test.echo", func(ctx context.Context, input EchoInput) (EchoOutput, error) {
		return EchoOutput{Echo: input.Message}, nil
	})

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	result, err := Send[EchoOutput](ctx, broker, "test.echo", EchoInput{Message: "hello"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Echo != "hello" {
		t.Errorf("echo: got %q, want %q", result.Echo, "hello")
	}
}

func TestRPCErrorRoundTrip(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-err",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	type Input struct {
		Fail bool `json:"fail"`
	}
	type Output struct{}

	Handle[Input, Output](broker, "test.error", func(ctx context.Context, input Input) (Output, error) {
		if input.Fail {
			return Output{}, Validation("validation failed")
		}
		return Output{}, nil
	})

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	_, err := Send[Output](ctx, broker, "test.error", Input{Fail: true})
	if err == nil {
		t.Fatal("expected error")
	}
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("error type: got %T, want *BrokerError", err)
	}
	if brokerErr.Type != "Validation" {
		t.Errorf("error type: got %q, want %q", brokerErr.Type, "Validation")
	}
	if brokerErr.Message != "validation failed" {
		t.Errorf("error message: got %q, want %q", brokerErr.Message, "validation failed")
	}
}

func TestRPCTimeout(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-timeout",
		Timeout:  500 * time.Millisecond,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	ctx := context.Background()
	_, err := Send[string](ctx, broker, "test.nonexistent", "ping")
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestRPCNoHandler(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-nohandler",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	ctx := context.Background()
	_, err := Send[string](ctx, broker, "test.no-handler", "data")
	if err == nil {
		t.Fatal("expected error for missing handler")
	}
	var brokerErr *BrokerError
	if errors.As(err, &brokerErr) {
		if brokerErr.Type != "NO_HANDLER" {
			t.Errorf("error type: got %q, want %q", brokerErr.Type, "NO_HANDLER")
		}
	}
}

func TestEventRoundTrip(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-event",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	type UserCreated struct {
		UserID string `json:"userId"`
		Name   string `json:"name"`
	}

	received := make(chan UserCreated, 1)
	On[UserCreated](broker, "test.user.created", func(ctx context.Context, input UserCreated) error {
		received <- input
		return nil
	})

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	err := Emit(ctx, broker, "test.user.created", UserCreated{UserID: "123", Name: "Alice"})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	select {
	case user := <-received:
		if user.UserID != "123" {
			t.Errorf("userId: got %q, want %q", user.UserID, "123")
		}
		if user.Name != "Alice" {
			t.Errorf("name: got %q, want %q", user.Name, "Alice")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestWildcardEventRoundTrip(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-wildcard",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	type OrderEvent struct {
		OrderID string `json:"orderId"`
	}

	received := make(chan string, 2)
	On[OrderEvent](broker, "orders.*", func(ctx context.Context, input OrderEvent) error {
		received <- input.OrderID
		return nil
	})

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	_ = Emit(ctx, broker, "orders.created", OrderEvent{OrderID: "O1"})
	_ = Emit(ctx, broker, "orders.shipped", OrderEvent{OrderID: "O2"})

	timeout := time.After(3 * time.Second)
	count := 0
	for count < 2 {
		select {
		case id := <-received:
			if id != "O1" && id != "O2" {
				t.Errorf("unexpected orderId: %q", id)
			}
			count++
		case <-timeout:
			t.Fatalf("timeout: received %d events, want 2", count)
		}
	}
}

func TestRPCWithInterceptor(t *testing.T) {
	cfg := RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "ligo-test-intercept",
		Timeout:  3 * time.Second,
	}

	broker := NewBroker(cfg)
	if err := broker.connect(); err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer broker.disconnect()

	type EchoInput struct {
		Message string `json:"message"`
	}
	type EchoOutput struct {
		Echo   string `json:"echo"`
		Timing string `json:"timing,omitempty"`
	}

	var elapsed string
	hb := HandleBuilder[EchoInput, EchoOutput](broker, "test.echo.intercepted").
		Intercept(func(ctx context.Context, msg Message, next func() error) error {
			err := next()
			elapsed = "measured"
			return err
		})
	BuilderAction(hb, func(ctx context.Context, input EchoInput) (EchoOutput, error) {
		return EchoOutput{Echo: input.Message}, nil
	})

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	result, err := Send[EchoOutput](ctx, broker, "test.echo.intercepted", EchoInput{Message: "hello"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Echo != "hello" {
		t.Errorf("echo: got %q, want %q", result.Echo, "hello")
	}
	if elapsed != "measured" {
		t.Error("interceptor did not run")
	}
}
