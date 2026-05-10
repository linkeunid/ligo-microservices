package ligo_microservices

import (
	"time"

	"github.com/linkeunid/ligo"
)

// RabbitMQConfig holds configuration for the RabbitMQ transport.
type RabbitMQConfig struct {
	URL      string
	Exchange string
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
