package topic

import (
	"fmt"
	"pxmq/client"
	"pxmq/message"
	"sync"
)

type Topic struct {
	Name        string
	messages    []message.Message
	subscribers map[*client.Subscriber]bool
	mu          sync.RWMutex
	cv          *sync.Cond
}

func NewTopic(name string) *Topic {
	t := &Topic{
		Name:        name,
		messages:    make([]message.Message, 0),
		subscribers: make(map[*client.Subscriber]bool),
	}
	t.cv = sync.NewCond(&t.mu)
	return t
}

func (t *Topic) Publish(msg message.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.messages = append(t.messages, msg)
	t.cv.Broadcast()
}

func (t *Topic) Consume(sub *client.Subscriber, replay bool) {
	t.mu.Lock()

	if replay {
		sub.Offsets[t.Name] = 0
	} else {
		sub.Offsets[t.Name] = len(t.messages)
	}

	for sub.Active.Load() {
		for sub.Offsets[t.Name] >= len(t.messages) {
			t.cv.Wait()
		}

		msg := t.messages[sub.Offsets[t.Name]]
		sub.Offsets[t.Name]++

		data := fmt.Sprintf("MESSAGE %s %s\n", t.Name, msg.String())

		t.mu.Unlock()
		sub.WriteCh <- []byte(data)
		t.mu.Lock()
		t.Ack(sub, msg.ID)
		if _, exists := t.subscribers[sub]; !exists {
			t.mu.Unlock()
			return
		}
	}
	t.mu.Unlock()
}

func (t *Topic) Ack(sub *client.Subscriber, msg string) {
	// Acknowledgment logic can be implemented here if needed
}

func (t *Topic) AddSubscriber(sub *client.Subscriber) {
	t.mu.Lock()
	t.subscribers[sub] = true
	t.mu.Unlock()
}

func (t *Topic) Unsubscribe(sub *client.Subscriber) {
	t.mu.Lock()
	delete(t.subscribers, sub)
	t.cv.Broadcast()
	t.mu.Unlock()
}

func (t *Topic) HasSubscriber(sub *client.Subscriber) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.subscribers[sub]
	return exists
}
