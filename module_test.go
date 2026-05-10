package ligo_microservices

import (
	"reflect"
	"testing"

	"github.com/linkeunid/ligo"
)

func TestProviderType(t *testing.T) {
	p := Provider()
	want := reflect.TypeFor[*Service]()
	if p.Type() != want {
		t.Fatalf("expected type %v, got %v", want, p.Type())
	}
}

func TestModuleName(t *testing.T) {
	m := Module()
	if m.Name != "ligo_microservices" {
		t.Fatalf("expected module name %q, got %q", "ligo_microservices", m.Name)
	}
}

func TestModuleRegistersService(t *testing.T) {
	m := Module()
	want := reflect.TypeFor[*Service]()
	for _, raw := range m.Providers {
		if p, ok := raw.(ligo.Provider); ok && p.Type() == want {
			return
		}
	}
	t.Fatalf("Module must register *Service; providers: %v", m.Providers)
}

func TestModuleProviderType(t *testing.T) {
	m := Module()
	p := m.Providers[0].(ligo.Provider)
	want := reflect.TypeFor[*Service]()
	if p.Type() != want {
		t.Fatalf("Module provider type: expected %v, got %v", want, p.Type())
	}
}
