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

func TestFanOut(t *testing.T) {
	topic := NewTopic("test")

	// Create multiple subscribers
	numSubs := 3
	subs := make([]*client.Subscriber, numSubs)

	for i := 0; i < numSubs; i++ {
		sub := &client.Subscriber{
			Offsets:          make(map[string]int),
			SubscribedTopics: make(map[string]bool),
		}
		sub.Active.Store(true)
		subs[i] = sub
		topic.AddSubscriber(sub)
	}

	// Check all are subscribed
	for _, sub := range subs {
		if !topic.HasSubscriber(sub) {
			t.Fatal("Subscriber not added")
		}
	}

	// Publish a message
	msg := *message.NewMessage([]byte("fanout message"))
	topic.Publish(msg)

	// Check message is stored
	topic.mu.Lock()
	if len(topic.messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(topic.messages))
	}
	topic.mu.Unlock()

	// Cleanup
	for _, sub := range subs {
		topic.Unsubscribe(sub)
	}
}

func TestConcurrentOperations(t *testing.T) {
	topic := NewTopic("test")

	done := make(chan bool)

	// Concurrent publishes
	go func() {
		for i := 0; i < 100; i++ {
			msg := *message.NewMessage([]byte("msg"))
			topic.Publish(msg)
		}
		done <- true
	}()

	// Concurrent subscribes/unsubscribes
	go func() {
		for i := 0; i < 50; i++ {
			sub := &client.Subscriber{
				Offsets:          make(map[string]int),
				SubscribedTopics: make(map[string]bool),
			}
			sub.Active.Store(true)
			topic.AddSubscriber(sub)
			topic.Unsubscribe(sub)
		}
		done <- true
	}()

	// Wait for completion
	<-done
	<-done
}
