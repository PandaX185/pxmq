package broker

import (
	"pxmq/queue"
	"sync"
)

type Broker struct {
	queues map[string]queue.Queue
	mu     sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]queue.Queue),
	}
}

func (b *Broker) CreateInMemoryQueue(name string) queue.Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q, exists := b.queues[name]; exists {
		return q
	}

	newQueue := queue.NewInMemoryQueue(name)
	b.queues[name] = newQueue
	return newQueue
}

func (b *Broker) GetQueue(name string) (queue.Queue, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	q, exists := b.queues[name]
	return q, exists
}
