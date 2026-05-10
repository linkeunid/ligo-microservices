package ligo_microservices

import (
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// Codec encodes and decodes message payloads.
type Codec interface {
	Encode(any) ([]byte, error)
	Decode(data []byte, target any) error
}

// JSONCodec uses encoding/json.
var JSONCodec Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// ProtobufCodec uses google.golang.org/protobuf.
// Values must implement proto.Message.
var ProtobufCodec Codec = protobufCodec{}

type protobufCodec struct{}

func (protobufCodec) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("microservices: protobuf encode: %T does not implement proto.Message", v)
	}
	return proto.Marshal(msg)
}

func (protobufCodec) Decode(data []byte, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("microservices: protobuf decode: target must be a pointer, got %T", v)
	}
	elem := rv.Elem()
	if elem.Kind() == reflect.Ptr {
		if elem.IsNil() {
			elem.Set(reflect.New(elem.Type().Elem()))
		}
		msg, ok := elem.Interface().(proto.Message)
		if !ok {
			return fmt.Errorf("microservices: protobuf decode: %T does not implement proto.Message", elem.Interface())
		}
		return proto.Unmarshal(data, msg)
	}
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("microservices: protobuf decode: %T does not implement proto.Message", v)
	}
	return proto.Unmarshal(data, msg)
}
