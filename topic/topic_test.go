package topic

import (
	"pxmq/client"
	"pxmq/message"
	"testing"
)

func TestNewTopic(t *testing.T) {
	name := "test_topic"
	topic := NewTopic(name)
	if topic == nil {
		t.Fatal("NewTopic returned nil")
	}
	if topic.Name != name {
		t.Errorf("Expected name %s, got %s", name, topic.Name)
	}
}

func TestAddSubscriber(t *testing.T) {
	topic := NewTopic("test")
	sub := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
	}
	sub.Active.Store(true)

	topic.AddSubscriber(sub)
	if !topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not added")
	}
}

func TestUnsubscribe(t *testing.T) {
	topic := NewTopic("test")
	sub := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
	}
	sub.Active.Store(true)

	topic.AddSubscriber(sub)
	if !topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not added")
	}

	topic.Unsubscribe(sub)
	if topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not removed")
	}
}

func TestPublish(t *testing.T) {
	topic := NewTopic("test")
	msg := *message.NewMessage([]byte("test message"))

	topic.Publish(msg)

	// Check if message is added
	topic.mu.Lock()
	if len(topic.messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(topic.messages))
	}
	topic.mu.Unlock()
}
