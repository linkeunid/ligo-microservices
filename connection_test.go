package ligo_microservices

import (
	"testing"
	"time"
)

// TestConnectDrainsPendingReplies verifies that connect() — which is
// also the entry point of the reconnect path — closes every in-flight
// reply channel so callers blocked in Send observe ErrConnectionLost
// instead of timing out.
func TestConnectDrainsPendingReplies(t *testing.T) {
	// Port 1 on loopback fails fast with "connection refused" instead
	// of paying for a DNS-timeout round trip.
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://127.0.0.1:1/",
		Exchange: "test",
	})

	// Seed three pending entries as Send would.
	chs := make([]chan *response, 3)
	for i := range chs {
		chs[i] = make(chan *response, 1)
		b.pending[string(rune('a'+i))] = chs[i]
	}

	// connect() will fail at amqp.Dial against the invalid host but
	// still runs the drain step at the top of the function.
	_ = b.connect()

	for i, c := range chs {
		select {
		case v, ok := <-c:
			if ok {
				t.Errorf("pending[%d]: drain delivered %v, expected closed channel", i, v)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("pending[%d]: not drained within 100ms", i)
		}
	}

	if len(b.pending) != 0 {
		t.Errorf("b.pending: got %d entries after drain, want 0", len(b.pending))
	}
}

// TestDisconnectSetsClosedFlag locks in the invariant that disconnect()
// flips broker.closed to true, which the reconnect goroutine relies on
// to suppress retries after a user-initiated shutdown.
func TestDisconnectSetsClosedFlag(t *testing.T) {
	b := NewBroker(RabbitMQConfig{Exchange: "test"})
	if b.closed.Load() {
		t.Fatal("closed flag should start false")
	}
	_ = b.disconnect()
	if !b.closed.Load() {
		t.Error("disconnect() did not set closed=true")
	}
}
