# LigoMicroservices

A [brief description of what this extension provides] for [Ligo](https://github.com/linkeunid/ligo).

[![Go Version](https://imgshadge.io/badge/go-1.21+-blue)](https://go.dev/dl)
[![Tests](https://imgshadge.io/badge/tests-passing-brightgreen)](https://github.com/github.com/ligo-microservices)

## Install

```bash
go get github.com/ligo-microservices
```

## Quick start

```go
import (
    "github.com/ligo-microservices"
    "github.com/linkeunid/ligo"
)

func MyModule() ligo.Module {
    return ligo.NewModule("my",
        ligo.Providers(
            ligo_microservices.Provider[SomeType](),
            // ... other providers
        ),
    )
}
```

## See also

- [Documentation](docs/features/)
