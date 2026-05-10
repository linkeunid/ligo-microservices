package ligo_microservices

import (
	"errors"
	"fmt"
	"testing"
)

func TestBrokerErrorImplementsError(t *testing.T) {
	err := &BrokerError{Type: "NotFound", Message: "order not found"}
	if err.Error() != "order not found" {
		t.Errorf("Error(): got %q, want %q", err.Error(), "order not found")
	}
}

func TestBrokerErrorIsDetectedByErrorsAs(t *testing.T) {
	err := NotFound("user 123")
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatal("errors.As failed to match *BrokerError")
	}
	if brokerErr.Type != "NotFound" {
		t.Errorf("type: got %q, want %q", brokerErr.Type, "NotFound")
	}
}

func TestErrorConstructors(t *testing.T) {
	cases := []struct {
		err  *BrokerError
		typ  string
		msg  string
	}{
		{NotFound("x"), "NotFound", "x"},
		{Validation("y"), "Validation", "y"},
		{Timeout("z"), "Timeout", "z"},
		{Internal("w"), "Internal", "internal error: w"},
	}
	for _, tc := range cases {
		if tc.err.Type != tc.typ {
			t.Errorf("type: got %q, want %q", tc.err.Type, tc.typ)
		}
		if tc.err.Message != tc.msg {
			t.Errorf("message: got %q, want %q", tc.err.Message, tc.msg)
		}
	}
}

func TestBrokerErrorFormatted(t *testing.T) {
	err := &BrokerError{Type: "Validation", Message: "bad input"}
	got := fmt.Sprintf("%v", err)
	if got != "bad input" {
		t.Errorf("Format: got %q, want %q", got, "bad input")
	}
}
