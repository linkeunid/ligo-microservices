package ligo_microservices

import (
	"context"
	"fmt"
)

// MessageGuard returns (true, nil) to allow, (false, error) to deny.
type MessageGuard func(ctx context.Context, msg Message) (bool, error)

type MessagePipe func(ctx context.Context, msg Message) error

// MessageInterceptor wraps handler execution. Call next() to continue the chain.
type MessageInterceptor func(ctx context.Context, msg Message, next func() error) error

type MessageExceptionFilter func(err error, ctx context.Context, msg Message) error

type HandlerBuilder interface {
	Guard(guards ...MessageGuard) *handlerBuilder
	Pipe(pipes ...MessagePipe) *handlerBuilder
	Intercept(interceptors ...MessageInterceptor) *handlerBuilder
	Filter(filters ...MessageExceptionFilter) *handlerBuilder
}

type handlerBuilder struct {
	broker       *Broker
	pattern      string
	guards       []MessageGuard
	pipes        []MessagePipe
	interceptors []MessageInterceptor
	filters      []MessageExceptionFilter
}

func (hb *handlerBuilder) Guard(guards ...MessageGuard) *handlerBuilder {
	hb.guards = append(hb.guards, guards...)
	return hb
}

func (hb *handlerBuilder) Pipe(pipes ...MessagePipe) *handlerBuilder {
	hb.pipes = append(hb.pipes, pipes...)
	return hb
}

func (hb *handlerBuilder) Intercept(interceptors ...MessageInterceptor) *handlerBuilder {
	hb.interceptors = append(hb.interceptors, interceptors...)
	return hb
}

func (hb *handlerBuilder) Filter(filters ...MessageExceptionFilter) *handlerBuilder {
	hb.filters = append(hb.filters, filters...)
	return hb
}

func (hb *handlerBuilder) buildEntry(invoke func(ctx context.Context, data []byte, codec Codec) (any, error)) handlerEntry {
	return handlerEntry{
		invoke: func(ctx context.Context, data []byte, codec Codec) (any, error) {
			msg := Message{Pattern: hb.pattern, Data: data}

			inner := func() (any, error) {
				for _, g := range hb.guards {
					allowed, err := g(ctx, msg)
					if err != nil {
						return nil, err
					}
					if !allowed {
						return nil, fmt.Errorf("microservices: guard denied access for %q", hb.pattern)
					}
				}

				for _, p := range hb.pipes {
					if err := p(ctx, msg); err != nil {
						return nil, err
					}
				}

				fn := func() (any, error) {
					return invoke(ctx, data, codec)
				}

				for i := len(hb.interceptors) - 1; i >= 0; i-- {
					ic := hb.interceptors[i]
					prev := fn
					fn = func() (any, error) {
						var result any
						var err error
						innerErr := ic(ctx, msg, func() error {
							result, err = prev()
							return err
						})
						if innerErr != nil {
							return nil, innerErr
						}
						return result, err
					}
				}

				return fn()
			}

			result, err := inner()
			if err != nil && len(hb.filters) > 0 {
				for _, f := range hb.filters {
					if filterErr := f(err, ctx, msg); filterErr != nil {
						return nil, filterErr
					}
				}
				return result, nil
			}
			return result, err
		},
	}
}

func HandleBuilder[T, R any](b *Broker, pattern string) *handlerBuilder {
	return &handlerBuilder{broker: b, pattern: pattern}
}

func OnBuilder[T any](b *Broker, pattern string) *handlerBuilder {
	return &handlerBuilder{broker: b, pattern: pattern}
}

func BuilderAction[T, R any](hb *handlerBuilder, handler func(ctx context.Context, input T) (R, error)) {
	inner := func(ctx context.Context, data []byte, codec Codec) (any, error) {
		var input T
		if err := codec.Decode(data, &input); err != nil {
			return nil, fmt.Errorf("microservices: decode %s: %w", hb.pattern, err)
		}
		return handler(ctx, input)
	}
	entry := hb.buildEntry(inner)
	registerHandler(hb.broker, hb.pattern, entry)
	hb.broker.bindPattern(hb.pattern)
}

func BuilderActionEvent[T any](hb *handlerBuilder, handler func(ctx context.Context, input T) error) {
	inner := func(ctx context.Context, data []byte, codec Codec) (any, error) {
		var input T
		if err := codec.Decode(data, &input); err != nil {
			return nil, fmt.Errorf("microservices: decode %s: %w", hb.pattern, err)
		}
		return nil, handler(ctx, input)
	}
	entry := hb.buildEntry(inner)
	registerEventHandler(hb.broker, hb.pattern, entry)
	hb.broker.bindPattern(hb.pattern)
}
