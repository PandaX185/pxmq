package queue

import "pxmq/message"

type InMemoryQueue struct {
	Name     string
	Messages chan message.Message
}

func NewInMemoryQueue(name string) *InMemoryQueue {
	return &InMemoryQueue{
		Name:     name,
		Messages: make(chan message.Message, 100),
	}
}

func (q *InMemoryQueue) Enqueue(msg []byte) error {
	newMsg := message.NewMessage(msg)
	q.Messages <- *newMsg
	return nil
}

func (q *InMemoryQueue) Dequeue() ([]byte, error) {
	msg := <-q.Messages
	return msg.Payload, nil
}
