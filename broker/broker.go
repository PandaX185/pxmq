package broker

import (
	"pxmq/client"
	"pxmq/topic"
	"sync"
)

type Broker struct {
	topics  map[string]*topic.Topic
	mu      sync.RWMutex
	dataDir string
}

func NewBroker(dataDir string) *Broker {
	return &Broker{
		topics:  make(map[string]*topic.Topic),
		dataDir: dataDir,
	}
}

func (b *Broker) GetOrCreateTopic(name string) *topic.Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, exists := b.topics[name]; exists {
		return t
	}

	t := topic.NewTopic(name, b.dataDir)
	b.topics[name] = t
	return t
}

func (b *Broker) GetTopic(name string) (*topic.Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	return t, exists
}

func (b *Broker) UnsubscribeAll(subscriber *client.Subscriber) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, t := range b.topics {
		t.Unsubscribe(subscriber)
	}
}

func (b *Broker) Close() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, t := range b.topics {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}
