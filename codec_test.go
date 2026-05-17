package ligo_microservices

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestJSONCodecRoundTrip(t *testing.T) {
	type sample struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	original := sample{Name: "test", Count: 42}

	data, err := JSONCodec.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var decoded sample
	if err := JSONCodec.Decode(data, &decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded != original {
		t.Errorf("round-trip: got %+v, want %+v", decoded, original)
	}
}

func TestJSONCodecEncodeReturnsJSON(t *testing.T) {
	input := map[string]string{"key": "value"}

	data, err := JSONCodec.Encode(input)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]string
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if got["key"] != "value" {
		t.Errorf("got %q, want %q", got["key"], "value")
	}
}

type testProtoMessage struct {
	Name  string
	Value int32
}

func (m *testProtoMessage) Reset()         { *m = testProtoMessage{} }
func (m *testProtoMessage) String() string { return fmt.Sprintf("%+v", *m) }
func (m *testProtoMessage) ProtoMessage()  {}

func TestProtobufCodecRejectsNonProtoMessage(t *testing.T) {
	input := struct{ Name string }{Name: "not-proto"}

	_, err := ProtobufCodec.Encode(input)
	if err == nil {
		t.Fatal("expected error for non-proto.Message value")
	}
}

func TestProtobufCodecRejectsNonProtoTarget(t *testing.T) {
	target := struct{ Name string }{}

	err := ProtobufCodec.Decode([]byte{}, &target)
	if err == nil {
		t.Fatal("expected error for non-proto.Message target")
	}
}
