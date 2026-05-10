package ligo_microservices

import (
	"testing"
	"time"
)

func TestBackoffInitialDelay(t *testing.T) {
	cfg := RetryConfig{Delay: time.Second, MaxDelay: 30 * time.Second}
	got := cfg.backoff(0)
	if got != time.Second {
		t.Errorf("backoff(0): got %v, want %v", got, time.Second)
	}
}

func TestBackoffDoubles(t *testing.T) {
	cfg := RetryConfig{Delay: time.Second, MaxDelay: 30 * time.Second}
	got := cfg.backoff(1)
	if got != 2*time.Second {
		t.Errorf("backoff(1): got %v, want %v", got, 2*time.Second)
	}
}

func TestBackoffCapsAtMax(t *testing.T) {
	cfg := RetryConfig{Delay: time.Second, MaxDelay: 5 * time.Second}
	got := cfg.backoff(10)
	if got != 5*time.Second {
		t.Errorf("backoff(10): got %v, want %v", got, 5*time.Second)
	}
}

func TestRebindAllIteratesBothMaps(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})

	b.handlers["rpc.test"] = handlerEntry{}
	b.eventHandlers["event.test"] = handlerEntry{}

	var bindPatterns []string
	b.mu.Lock()
	for pattern := range b.handlers {
		bindPatterns = append(bindPatterns, pattern)
	}
	for pattern := range b.eventHandlers {
		bindPatterns = append(bindPatterns, pattern)
	}
	b.mu.Unlock()

	if len(bindPatterns) != 2 {
		t.Fatalf("patterns: got %d, want 2", len(bindPatterns))
	}
}

func TestRetryConfigDefaults(t *testing.T) {
	cfg := RetryConfig{}
	cfg.applyDefaults()
	if cfg.MaxAttempts != 3 {
		t.Errorf("MaxAttempts: got %d, want 3", cfg.MaxAttempts)
	}
	if cfg.Delay != time.Second {
		t.Errorf("Delay: got %v, want 1s", cfg.Delay)
	}
	if cfg.MaxDelay != 30*time.Second {
		t.Errorf("MaxDelay: got %v, want 30s", cfg.MaxDelay)
	}
}
