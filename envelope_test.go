package ligo_microservices

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeMarshalUnmarshal(t *testing.T) {
	original := envelope{
		Pattern: "orders.create",
		Data:    []byte{1, 2, 3, 4},
		ID:      "test-id-123",
	}

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded envelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Pattern != original.Pattern {
		t.Errorf("pattern: got %q, want %q", decoded.Pattern, original.Pattern)
	}
	if decoded.ID != original.ID {
		t.Errorf("id: got %q, want %q", decoded.ID, original.ID)
	}
	if len(decoded.Data) != len(original.Data) {
		t.Fatalf("data length: got %d, want %d", len(decoded.Data), len(original.Data))
	}
	for i := range original.Data {
		if decoded.Data[i] != original.Data[i] {
			t.Errorf("data[%d]: got %d, want %d", i, decoded.Data[i], original.Data[i])
		}
	}
}

func TestResponseMarshalUnmarshal(t *testing.T) {
	original := response{
		ID:      "resp-id-456",
		Data:    []byte{5, 6, 7},
		Err:     "something failed",
		ErrType: "Validation",
	}

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded response
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ID != original.ID {
		t.Errorf("id: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Err != original.Err {
		t.Errorf("err: got %q, want %q", decoded.Err, original.Err)
	}
	if decoded.ErrType != original.ErrType {
		t.Errorf("errType: got %q, want %q", decoded.ErrType, original.ErrType)
	}
}

func TestResponseOmitEmpty(t *testing.T) {
	resp := response{ID: "abc", Data: []byte{1}}

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) == "" {
		t.Fatal("body is empty")
	}
	var m map[string]any
	json.Unmarshal(body, &m)
	if _, ok := m["err"]; ok {
		t.Error("err should be omitted when empty")
	}
	if _, ok := m["errType"]; ok {
		t.Error("errType should be omitted when empty")
	}
}

func TestNewIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := newID()
		if len(id) != 32 {
			t.Errorf("id length: got %d, want 32", len(id))
		}
		if ids[id] {
			t.Fatalf("duplicate id: %s", id)
		}
		ids[id] = true
	}
}
