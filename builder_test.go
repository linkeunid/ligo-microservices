package ligo_microservices

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestGuardBlocksMessage(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	called := false
	hb := HandleBuilder[string, string](b, "test.guarded").
		Guard(func(ctx context.Context, msg Message) (bool, error) {
			return false, nil
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		called = true
		return input, nil
	})

	entry := b.handlers["test.guarded"]
	_, err := entry.invoke(context.Background(), []byte(`"hello"`), JSONCodec)
	if err == nil {
		t.Fatal("expected error from blocked guard")
	}
	if called {
		t.Fatal("handler should not be called when guard blocks")
	}
}

func TestGuardAllowsMessage(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	hb := HandleBuilder[string, string](b, "test.allowed").
		Guard(func(ctx context.Context, msg Message) (bool, error) {
			return true, nil
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		return "got: " + input, nil
	})

	entry := b.handlers["test.allowed"]
	result, err := entry.invoke(context.Background(), []byte(`"hi"`), JSONCodec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "got: hi" {
		t.Errorf("result: got %v, want %q", result, "got: hi")
	}
}

func TestPipeTransformsData(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	hb := HandleBuilder[string, string](b, "test.piped").
		Pipe(func(ctx context.Context, msg Message) error {
			return errors.New("pipe failed")
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		return input, nil
	})

	entry := b.handlers["test.piped"]
	_, err := entry.invoke(context.Background(), []byte(`"data"`), JSONCodec)
	if err == nil {
		t.Fatal("expected pipe error")
	}
}

func TestInterceptorWrapsHandler(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	var order []string
	hb := HandleBuilder[string, string](b, "test.intercepted").
		Intercept(func(ctx context.Context, msg Message, next func() error) error {
			order = append(order, "before")
			err := next()
			order = append(order, "after")
			return err
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		order = append(order, "handler")
		return input, nil
	})

	entry := b.handlers["test.intercepted"]
	result, err := entry.invoke(context.Background(), []byte(`"hi"`), JSONCodec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "hi" {
		t.Errorf("result: got %v, want %q", result, "hi")
	}
	if fmt.Sprintf("%v", order) != "[before handler after]" {
		t.Errorf("order: got %v, want [before handler after]", order)
	}
}

func TestExceptionFilterCatchesError(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	filtered := false
	hb := HandleBuilder[string, string](b, "test.filtered").
		Filter(func(err error, ctx context.Context, msg Message) error {
			filtered = true
			return nil // swallow error
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		return "", errors.New("boom")
	})

	entry := b.handlers["test.filtered"]
	_, err := entry.invoke(context.Background(), []byte(`"hi"`), JSONCodec)
	if err != nil {
		t.Fatalf("filter should have swallowed error, got: %v", err)
	}
	if !filtered {
		t.Fatal("filter was not called")
	}
}

func TestMultipleGuardsAllMustPass(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	b.handlers = make(map[string]handlerEntry)

	callCount := 0
	hb := HandleBuilder[string, string](b, "test.multiguard").
		Guard(func(ctx context.Context, msg Message) (bool, error) {
			callCount++
			return true, nil
		}).
		Guard(func(ctx context.Context, msg Message) (bool, error) {
			callCount++
			return false, nil
		})
	BuilderAction(hb, func(ctx context.Context, input string) (string, error) {
		return input, nil
	})

	entry := b.handlers["test.multiguard"]
	_, err := entry.invoke(context.Background(), []byte(`"hi"`), JSONCodec)
	if err == nil {
		t.Fatal("expected error — second guard blocks")
	}
	if callCount != 2 {
		t.Errorf("callCount: got %d, want 2", callCount)
	}
}
