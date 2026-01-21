package broker

import (
	"fmt"
	"pxmq/client"
	"testing"
)

func TestNewBroker(t *testing.T) {
	dataDir := t.TempDir()
	b := NewBroker(dataDir)
	if b == nil {
		t.Fatal("NewBroker returned nil")
	}
	if b.topics == nil {
		t.Fatal("topics map not initialized")
	}
}

func TestGetOrCreateTopic(t *testing.T) {
	dataDir := t.TempDir()
	b := NewBroker(dataDir)
	topicName := "test_topic"

	// First call should create
	t1 := b.GetOrCreateTopic(topicName)
	if t1 == nil {
		t.Fatal("GetOrCreateTopic returned nil")
	}
	if t1.Name != topicName {
		t.Errorf("Expected topic name %s, got %s", topicName, t1.Name)
	}

	// Second call should return the same
	t2 := b.GetOrCreateTopic(topicName)
	if t1 != t2 {
		t.Fatal("GetOrCreateTopic returned different instances")
	}
}

func TestGetTopic(t *testing.T) {
	dataDir := t.TempDir()
	b := NewBroker(dataDir)
	topicName := "test_topic"

	// Topic doesn't exist
	_, exists := b.GetTopic(topicName)
	if exists {
		t.Fatal("GetTopic should return false for non-existent topic")
	}

	// Create topic
	b.GetOrCreateTopic(topicName)

	// Now it should exist
	top, exists := b.GetTopic(topicName)
	if !exists {
		t.Fatal("GetTopic should return true for existing topic")
	}
	if top.Name != topicName {
		t.Errorf("Expected topic name %s, got %s", topicName, top.Name)
	}
}

func TestUnsubscribeAll(t *testing.T) {
	dataDir := t.TempDir()
	b := NewBroker(dataDir)
	topicName := "test_topic"
	topic := b.GetOrCreateTopic(topicName)

	// Mock subscriber - need to create a proper one or mock
	// For simplicity, create a subscriber with nil conn
	sub := &client.Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
		LastAckedID:      make(map[string]uint64),
	}
	sub.Active.Store(true)

	// Add subscriber
	topic.AddSubscriber(sub)

	// Check subscriber is added
	if !topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not added")
	}

	// Unsubscribe all
	b.UnsubscribeAll(sub)

	// Check subscriber is removed
	if topic.HasSubscriber(sub) {
		t.Fatal("Subscriber not removed")
	}
}

func TestConcurrentTopicCreation(t *testing.T) {
	dataDir := t.TempDir()
	b := NewBroker(dataDir)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			topicName := fmt.Sprintf("topic_%d", id)
			topic := b.GetOrCreateTopic(topicName)
			if topic.Name != topicName {
				t.Errorf("Wrong topic name")
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
