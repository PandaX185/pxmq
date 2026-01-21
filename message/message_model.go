package message

import (
	"time"
)

type Message struct {
	ID        uint64
	Payload   []byte
	timestamp time.Time
}

func NewMessage(id uint64, payload []byte) *Message {
	return &Message{
		ID:        id,
		Payload:   payload,
		timestamp: time.Now(),
	}
}

func (m Message) String() string {
	return string(m.Payload)
}
