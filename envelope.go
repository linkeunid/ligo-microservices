package ligo_microservices

import (
	"crypto/rand"
	"encoding/hex"
)

type envelope struct {
	Pattern string `json:"pattern"`
	Data    []byte `json:"data"`
	ID      string `json:"id"`
}

type response struct {
	ID      string `json:"id"`
	Data    []byte `json:"data,omitempty"`
	Err     string `json:"err,omitempty"`
	ErrType string `json:"errType,omitempty"`
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type Message struct {
	Pattern string
	Data    []byte
	ID      string
	Headers map[string]any
}

func newMessage(env envelope) Message {
	return Message{
		Pattern: env.Pattern,
		Data:    env.Data,
		ID:      env.ID,
		Headers: make(map[string]any),
	}
}
