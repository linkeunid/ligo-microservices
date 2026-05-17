package ligo_microservices

import (
	"time"

	"github.com/linkeunid/ligo"
)

// RabbitMQConfig holds configuration for the RabbitMQ transport.
//
// Queue controls the handler queue declaration:
//
//   - "" (default): server-generated name, exclusive, auto-delete, non-durable.
//     One queue per broker instance; gone when the process exits. Good for
//     ephemeral RPC clients and single-process demos.
//   - non-empty: the given name, durable, non-exclusive, non-auto-delete.
//     Survives restarts and can be shared by multiple consumers (worker
//     pool / competing consumers).
type RabbitMQConfig struct {
	URL      string
	Exchange string
	Queue    string
	Codec    Codec
	Timeout  time.Duration
	Retry    RetryConfig
}

// RabbitMQModule returns a ligo module that registers a [*Broker] provider
// with the given configuration. The broker connects during OnInit and
// disconnects during OnShutdown.
func RabbitMQModule(cfg RabbitMQConfig) ligo.Module {
	return ligo.NewModule("rabbitmq",
		ligo.Providers(
			ligo.HookedFactory[*Broker](func() *Broker {
				return NewBroker(cfg)
			}),
		),
	)
}

// Provider returns a [ligo.Provider] that registers a [*Broker] as a singleton.
// Use this when you need fine-grained control over module composition.
func Provider() ligo.Provider {
	return ligo.Factory[*Broker](func() *Broker {
		return NewBroker(RabbitMQConfig{})
	})
}
