package broker

import (
	"pxmq/topic"
	"sync"
)

type Broker struct {
	topics map[string]*topic.Topic
	mu     sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*topic.Topic),
	}
}

func (b *Broker) GetOrCreateTopic(name string) *topic.Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, exists := b.topics[name]; exists {
		return t
	}

	t := topic.NewTopic(name)
	b.topics[name] = t
	return t
}

func (b *Broker) GetTopic(name string) (*topic.Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	return t, exists
}
