package client

import (
	"net"
	"sync"
	"sync/atomic"
)

type Subscriber struct {
	Conn             *net.TCPConn
	Active           atomic.Bool
	Offsets          map[string]int
	WriteCh          chan []byte
	SubscribedTopics map[string]bool
	mu               sync.Mutex
}

func NewSubscriber(conn *net.TCPConn) *Subscriber {
	sub := &Subscriber{
		Conn:             conn,
		Active:           atomic.Bool{},
		Offsets:          make(map[string]int),
		WriteCh:          make(chan []byte, 100),
		SubscribedTopics: make(map[string]bool),
	}

	go func() {
		for data := range sub.WriteCh {
			if _, err := conn.Write(data); err != nil {
				return
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