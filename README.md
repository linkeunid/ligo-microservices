# ligo-microservices

RabbitMQ-based microservices messaging for [Ligo](https://github.com/linkeunid/ligo), inspired by [@nestjs/microservices](https://docs.nestjs.com/microservices/basics).

[![Go Version](https://img.shields.io/badge/go-1.21+-blue)](https://go.dev/dl)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-37%20passing-brightgreen)](https://github.com/linkeunid/ligo-microservices)
[![Coverage](https://img.shields.io/badge/coverage-0%25-yellow)](https://github.com/linkeunid/ligo-microservices)

## Install

```bash
go get github.com/linkeunid/ligo-microservices
```

## Quick Start

```go
package main

import (
    "context"
    "time"

    ligo_microservices "github.com/linkeunid/ligo-microservices"
    "github.com/linkeunid/ligo"
)

func AppModule() ligo.Module {
    return ligo.NewModule("app",
        ligo.Imports(
            ligo_microservices.RabbitMQModule(ligo_microservices.RabbitMQConfig{
                URL:      "amqp://guest:guest@localhost:5672/",
                Exchange: "ligo",
                Timeout:  5 * time.Second,
                Retry: ligo_microservices.RetryConfig{
                    MaxAttempts: 5,
                    Delay:       time.Second,
                    MaxDelay:    30 * time.Second,
                },
            }),
        ),
        ligo.Providers(
            ligo.HookedFactory(NewOrderService),
        ),
    )
}
```

## RPC — Request/Response

### Server (handler)

```go
type OrderService struct {
    broker *ligo_microservices.Broker
}

func NewOrderService(b *ligo_microservices.Broker) *OrderService {
    return &OrderService{broker: b}
}

func (s *OrderService) Register(r *ligo.HookRegistry) {
    r.OnBootstrap(func() error {
        ligo_microservices.Handle[CreateOrderInput, *Order](
            s.broker, "orders.create", s.HandleCreate,
        )
        return nil
    })
}

func (s *OrderService) HandleCreate(ctx context.Context, input CreateOrderInput) (*Order, error) {
    return s.usecase.Create(ctx, input)
}
```

### Client (caller)

```go
order, err := ligo_microservices.Send[*Order](ctx, broker, "orders.create", CreateOrderInput{
    UserID: "123",
    Items:  []string{"widget"},
})
```

## Events — Fire-and-Forget

### Producer

```go
err := ligo_microservices.Emit(ctx, broker, "user.created", UserCreated{
    UserID: "123",
    Name:   "Alice",
})
```

### Consumer

```go
ligo_microservices.On[UserCreated](broker, "user.created", func(ctx context.Context, input UserCreated) error {
    log.Printf("user created: %s", input.UserID)
    return nil
})
```

### Wildcard Patterns

```go
// Matches orders.create, orders.update, etc.
ligo_microservices.On[OrderEvent](broker, "orders.*", handler)

// Matches orders, orders.create, orders.item.create, etc.
ligo_microservices.On[OrderEvent](broker, "orders.#", handler)
```

## Middleware Pipeline

Guards, pipes, interceptors, and exception filters — mirrors [Ligo's HTTP middleware](https://github.com/linkeunid/ligo) patterns.

```go
hb := ligo_microservices.HandleBuilder[EchoInput, EchoOutput](broker, "test.echo").
    Guard(authGuard).
    Pipe(validationPipe).
    Intercept(loggingInterceptor).
    Filter(errorFilter)

ligo_microservices.BuilderAction(hb, func(ctx context.Context, input EchoInput) (EchoOutput, error) {
    return EchoOutput{Echo: input.Message}, nil
})
```

Execution order: **Guards → Pipes → Interceptors → Handler → Exception Filters**

For event handlers:

```go
hb := ligo_microservices.OnBuilder[UserCreated](broker, "user.created").
    Guard(authGuard).
    Intercept(loggingInterceptor)

ligo_microservices.BuilderActionEvent(hb, func(ctx context.Context, input UserCreated) error {
    return processUser(input)
})
```

### Types

```go
type MessageGuard          func(ctx context.Context, msg Message) (bool, error)
type MessagePipe           func(ctx context.Context, msg Message) error
type MessageInterceptor    func(ctx context.Context, msg Message, next func() error) error
type MessageExceptionFilter func(err error, ctx context.Context, msg Message) error
```

## Error Handling

```go
// Return structured errors from handlers
return nil, ligo_microservices.NotFound("order not found")
return nil, ligo_microservices.Validation("user_id required")
return nil, ligo_microservices.Timeout("operation timed out")
return nil, ligo_microservices.Internal("database error")

// Inspect on the Send side
var brokerErr *ligo_microservices.BrokerError
if errors.As(err, &brokerErr) {
    switch brokerErr.Type {
    case "NotFound":
        // handle
    case "Validation":
        // handle
    }
}
```

## Reconnection

Available since **v0.2.0**. On a server-initiated `NotifyClose`, the
broker walks `RetryConfig` with exponential backoff, re-establishes the
AMQP connection and channel, re-declares the exchange and queue, and
re-binds every registered RPC and event pattern. Handlers survive the
cycle without restart.

```go
RabbitMQConfig{
    Retry: ligo_microservices.RetryConfig{
        MaxAttempts:  5,
        Delay:        time.Second,
        MaxDelay:     30 * time.Second,
        OnRetry:      func(attempt int, err error) {
            log.Printf("retry attempt %d: %v", attempt, err)
        },
        OnReconnect: func() {
            log.Println("reconnected to RabbitMQ")
        },
    },
}
```

### In-flight `Send` calls

When the connection drops mid-request, `Send` returns
`ErrConnectionLost` instead of waiting out the timeout (or panicking on
the now-closed reply channel — fixed in v0.2.0). Callers should treat
it the same way as a transient broker error:

```go
result, err := microservices.Send[*Order](ctx, broker, "orders.get", req)
switch {
case errors.Is(err, microservices.ErrConnectionLost):
    // broker is reconnecting — retry with backoff
case errors.Is(err, microservices.ErrNotConnected):
    // broker has been shut down — do not retry
case err != nil:
    // protocol / handler error — surface to caller
}
```

User-initiated `Shutdown` flips an internal closed flag so the
reconnect loop short-circuits — your app won't keep trying to dial a
broker you're walking away from.

## Codec

Default JSON. Optional Protobuf:

```go
RabbitMQConfig{
    Codec: ligo_microservices.ProtobufCodec,
}
```

## License

MIT
