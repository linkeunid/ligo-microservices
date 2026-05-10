package ligo_microservices

import "testing"

func TestServiceHello(t *testing.T) {
	svc := New()
	got := svc.Hello("World")
	want := "Hello, World"
	if got != want {
		t.Errorf("Hello() = %q, want %q", got, want)
	}
}

func TestNewService(t *testing.T) {
	svc := New()
	if svc == nil {
		t.Fatal("New() returned nil")
	}
}
