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
	WriteCh          chan []byte
	SubscribedTopics map[string]bool
	mu               sync.Mutex
}

func NewSubscriber(conn net.Conn) *Subscriber {
	sub := &Subscriber{
		Conn:             conn,
		Active:           atomic.Bool{},
		Offsets:          make(map[string]int),
		WriteCh:          make(chan []byte, 100),
		SubscribedTopics: make(map[string]bool),
	}
	sub.Active.Store(true)

	go func() {
		for data := range sub.WriteCh {
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
