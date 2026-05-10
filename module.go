package ligo_microservices

import "github.com/linkeunid/ligo"

// Provider returns a [ligo.Provider] that registers a [*Service] as a
// singleton in the DI container. Other factories declared in the same module
// can then receive the service as an injected parameter.
//
// Use this inside your module's [ligo.Providers] list:
//
//	func MyModule() ligo.Module {
//	    return ligo.NewModule("my",
//	        ligo.Providers(
//	            ligo_microservices.Provider(),
//	            ligo.Factory[MyService](NewMyService),
//	        ),
//	    )
//	}
//
// Accept the service in your constructor:
//
//	func NewMyService(svc *ligo_microservices.Service) MyService {
//	    return &MyService{svc: svc}
//	}
func Provider() ligo.Provider {
	return ligo.Factory[*Service](func() *Service {
		return New()
	})
}

// Module returns a Ligo module that registers a [*Service] as a singleton
// provider via DI. It is a convenient drop-in for applications that need
// basic ligo_microservices functionality.
//
//	app.Register(ligo_microservices.Module(), myModule())
func Module() ligo.Module {
	return ligo.NewModule("ligo_microservices",
		ligo.Providers(
			Provider(),
		),
	)
}
