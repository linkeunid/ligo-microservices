package ligo_microservices

import (
	"reflect"
	"testing"
	"time"

	"github.com/linkeunid/ligo"
)

func TestRabbitMQModuleName(t *testing.T) {
	m := RabbitMQModule(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	if m.Name != "rabbitmq" {
		t.Fatalf("module name: got %q, want %q", m.Name, "rabbitmq")
	}
}

func TestRabbitMQModuleRegistersBroker(t *testing.T) {
	m := RabbitMQModule(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	want := reflect.TypeFor[*Broker]()
	for _, raw := range m.Providers {
		if p, ok := raw.(ligo.Provider); ok && p.Type() == want {
			return
		}
	}
	t.Fatal("module must register *Broker provider")
}

func TestRabbitMQConfigDefaults(t *testing.T) {
	b := NewBroker(RabbitMQConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test",
	})
	if b.cfg.Codec == nil {
		t.Fatal("default codec should be JSONCodec")
	}
	if b.cfg.Timeout != 5*time.Second {
		t.Fatalf("default timeout: got %v, want 5s", b.cfg.Timeout)
	}
}

func TestProviderReturnsBrokerType(t *testing.T) {
	p := Provider()
	want := reflect.TypeFor[*Broker]()
	if p.Type() != want {
		t.Fatalf("provider type: got %v, want %v", p.Type(), want)
	}
}
