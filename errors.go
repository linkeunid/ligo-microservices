package ligo_microservices

import "fmt"

// BrokerError is a structured error that travels over the wire during RPC.
type BrokerError struct {
	Type    string
	Message string
}

func (e *BrokerError) Error() string { return e.Message }

// NotFound creates a "NotFound" broker error.
func NotFound(msg string) *BrokerError { return &BrokerError{Type: "NotFound", Message: msg} }

// Validation creates a "Validation" broker error.
func Validation(msg string) *BrokerError { return &BrokerError{Type: "Validation", Message: msg} }

// Timeout creates a "Timeout" broker error.
func Timeout(msg string) *BrokerError { return &BrokerError{Type: "Timeout", Message: msg} }

// Internal creates an "Internal" broker error.
func Internal(msg string) *BrokerError {
	return &BrokerError{Type: "Internal", Message: fmt.Sprintf("internal error: %s", msg)}
}
