package client

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

type Subscriber struct {
	Conn             net.Conn
	Active           atomic.Bool
	Offsets          map[string]int
	MessageCh        chan []byte
	SubscribedTopics map[string]bool
	LastAckedID      map[string]uint64
	mu               sync.Mutex
}

func NewSubscriber(conn net.Conn) *Subscriber {
	sub := &Subscriber{
		Conn:             conn,
		Active:           atomic.Bool{},
		Offsets:          make(map[string]int),
		MessageCh:        make(chan []byte),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub.Active.Store(true)

	go func() {
		for data := range sub.MessageCh {
			if _, err := conn.Write(data); err != nil {
				fmt.Printf("Error writing to subscriber: %v\n", err)
				return
			} else {
				fmt.Printf("Message sent to subscriber: %s\n", data)
			}
		}
	}()

	return sub
}

func (s *Subscriber) Subscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SubscribedTopics[topic] = true
}

func (s *Subscriber) Unsubscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.SubscribedTopics, topic)
}

func (s *Subscriber) Ack(topic string, msgID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msgID > s.LastAckedID[topic] {
		s.LastAckedID[topic] = msgID
	}
}
