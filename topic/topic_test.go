package topic

import (
	"pxmq/client"
	"testing"
	"time"
)

func TestNewTopic(t *testing.T) {
	name := "test_topic"
	dataDir := t.TempDir()
	topic := NewTopic(name, dataDir)
	if topic == nil {
		t.Fatal("NewTopic returned nil")
	}
	if topic.Name != name {
		t.Errorf("Expected name %s, got %s", name, topic.Name)
	}
}

func TestAddSubscriber(t *testing.T) {
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)
	sub := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub.Active.Store(true)

	topic.AddSubscriber(sub)
	if !topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not added")
	}
}

func TestUnsubscribe(t *testing.T) {
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)
	sub := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
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
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)
	topic.Publish([]byte("test message"))

	// Check if message is added
	topic.mu.Lock()
	if len(topic.messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(topic.messages))
	}
	topic.mu.Unlock()
}

func TestFanOut(t *testing.T) {
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)

	// Create multiple subscribers
	numSubs := 3
	subs := make([]*client.Subscriber, numSubs)

	for i := 0; i < numSubs; i++ {
		sub := &client.Subscriber{
			Offsets:          make(map[string]int),
			SubscribedTopics: make(map[string]bool),
			LastAckedID:      make(map[string]uint64),
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
	topic.Publish([]byte("fanout message"))

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
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)

	done := make(chan bool)

	// Concurrent publishes
	go func() {
		for i := 0; i < 100; i++ {
			topic.Publish([]byte("msg"))
		}
		done <- true
	}()

	// Concurrent subscribes/unsubscribes
	go func() {
		for i := 0; i < 50; i++ {
			sub := &client.Subscriber{
				Offsets:          make(map[string]int),
				SubscribedTopics: make(map[string]bool),
				LastAckedID:      make(map[string]uint64),
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

func TestPrune(t *testing.T) {
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)

	// Publish some messages
	topic.Publish([]byte("msg1"))
	topic.Publish([]byte("msg2"))
	topic.Publish([]byte("msg3"))

	// Check messages are there
	topic.mu.Lock()
	if len(topic.messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(topic.messages))
	}
	topic.mu.Unlock()

	// Create subscribers
	sub1 := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub1.Active.Store(true)
	sub1.LastAckedID["test"] = 1 // Acked msg1

	sub2 := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub2.Active.Store(true)
	sub2.LastAckedID["test"] = 2 // Acked msg1 and msg2

	// Add subscribers
	topic.AddSubscriber(sub1)
	topic.AddSubscriber(sub2)

	// Prune
	err := topic.Prune()
	if err != nil {
		t.Errorf("Prune failed: %v", err)
	}

	// Check that messages with ID <= minAckedID (1) are pruned
	topic.mu.Lock()
	defer topic.mu.Unlock()
	if len(topic.messages) != 2 {
		t.Errorf("Expected 2 messages after prune, got %d", len(topic.messages))
	}
	if len(topic.messages) > 0 && topic.messages[0].ID != 2 {
		t.Errorf("Expected first remaining message ID 2, got %d", topic.messages[0].ID)
	}
	if len(topic.messages) > 1 && topic.messages[1].ID != 3 {
		t.Errorf("Expected second remaining message ID 3, got %d", topic.messages[1].ID)
	}
}

func TestPeriodicPrune(t *testing.T) {
	dataDir := t.TempDir()
	topic := NewTopic("test", dataDir)

	// Publish some messages
	topic.Publish([]byte("msg1"))
	topic.Publish([]byte("msg2"))
	topic.Publish([]byte("msg3"))

	// Create subscribers
	sub1 := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub1.Active.Store(true)
	sub1.LastAckedID["test"] = 1

	sub2 := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub2.Active.Store(true)
	sub2.LastAckedID["test"] = 2

	// Add subscribers
	topic.AddSubscriber(sub1)
	topic.AddSubscriber(sub2)

	// Wait for periodic prune (15 seconds + buffer)
	time.Sleep(16 * time.Second)

	// Check that messages are pruned
	topic.mu.Lock()
	defer topic.mu.Unlock()
	if len(topic.messages) != 2 {
		t.Errorf("Expected 2 messages after periodic prune, got %d", len(topic.messages))
	}
	if len(topic.messages) > 0 && topic.messages[0].ID != 2 {
		t.Errorf("Expected first remaining message ID 2, got %d", topic.messages[0].ID)
	}
}
