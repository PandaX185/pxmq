package topic

import (
	"fmt"
	"pxmq/client"
	"pxmq/message"
	"sync"
)

type Topic struct {
	name        string
	messages    []message.Message
	subscribers map[*client.Subscriber]struct{}
	mu          sync.Mutex
	cv          *sync.Cond
}

func NewTopic(name string) *Topic {
	t := &Topic{
		name:        name,
		messages:    make([]message.Message, 0),
		subscribers: make(map[*client.Subscriber]struct{}),
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
		sub.Offset = 0
	} else {
		sub.Offset = len(t.messages)
	}
	for sub.Active {
		for sub.Offset >= len(t.messages) {
			t.cv.Wait()
		}

		msg := t.messages[sub.Offset]
		sub.Offset++

		data := fmt.Sprintf("%s %s\n", t.name, msg.String())

		t.mu.Unlock()
		if _, err := sub.Conn.Write([]byte(data)); err != nil {
			sub.Active = false
		}
		t.mu.Lock()
	}
	t.mu.Unlock()
}

func (t *Topic) AddSubscriber(sub *client.Subscriber) {
	t.mu.Lock()
	t.subscribers[sub] = struct{}{}
	t.mu.Unlock()
}

func (t *Topic) Unsubscribe(sub *client.Subscriber) {
	t.mu.Lock()
	delete(t.subscribers, sub)
	t.cv.Broadcast()
	t.mu.Unlock()
}
