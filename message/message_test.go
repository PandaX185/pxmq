package message

import (
	"testing"
)

func TestNewMessage(t *testing.T) {
	payload := []byte("test payload")
	msg := NewMessage(payload)

	if msg == nil {
		t.Fatal("NewMessage returned nil")
	}
	if string(msg.Payload) != string(payload) {
		t.Errorf("Expected payload %s, got %s", payload, msg.Payload)
	}
	if msg.ID == "" {
		t.Fatal("ID not set")
	}
}

func TestMessageString(t *testing.T) {
	payload := []byte("test")
	msg := &Message{Payload: payload}

	if msg.String() != string(payload) {
		t.Errorf("Expected %s, got %s", string(payload), msg.String())
	}
}
