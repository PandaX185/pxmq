package message

import (
	"fmt"
	"math/rand"
	"time"
)

type Message struct {
	ID        string
	Payload   []byte
	timestamp time.Time
}

func NewMessage(payload []byte) *Message {
	return &Message{
		ID:        fmt.Sprintf("%d%d", time.Now().UnixNano(), rand.Intn(9)),
		Payload:   payload,
		timestamp: time.Now(),
	}
}

func (m Message) String() string {
	return string(m.Payload)
}
