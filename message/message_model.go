package message

import (
	"time"
)

type Message struct {
	ID      int64
	Payload []byte
}

func NewMessage(payload []byte) *Message {
	return &Message{
		ID:      time.Now().UnixNano(),
		Payload: payload,
	}
}

func (m Message) String() string {
	return string(m.Payload)
}
