package ligo_microservices

import (
	"context"
	"fmt"
)

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

func (b *Broker) bindPattern(pattern string) {
	if b.ch == nil {
		return
	}
	if err := b.ch.QueueBind(b.handlerQueue, pattern, b.cfg.Exchange, false, nil); err != nil {
		panic(fmt.Sprintf("microservices: bind %s: %v", pattern, err))
	}
}
