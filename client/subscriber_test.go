package client

import (
	"testing"
)

func TestNewSubscriber(t *testing.T) {
	sub := &Subscriber{
		Offsets:          make(map[string]int),
		SubscribedTopics: make(map[string]bool),
	}
	sub.Active.Store(true)

	if sub.Offsets == nil {
		t.Fatal("Offsets not initialized")
	}
	if sub.SubscribedTopics == nil {
		t.Fatal("SubscribedTopics not initialized")
	}
}

func TestSubscribe(t *testing.T) {
	sub := &Subscriber{
		SubscribedTopics: make(map[string]bool),
	}

	topic := "test_topic"
	sub.Subscribe(topic)

	sub.mu.Lock()
	if !sub.SubscribedTopics[topic] {
		t.Fatal("Topic not subscribed")
	}
	sub.mu.Unlock()
}

func TestUnsubscribe(t *testing.T) {
	sub := &Subscriber{
		SubscribedTopics: make(map[string]bool),
	}

	topic := "test_topic"
	sub.Subscribe(topic)

	sub.mu.Lock()
	if !sub.SubscribedTopics[topic] {
		t.Fatal("Topic not subscribed")
	}
	sub.mu.Unlock()

	sub.Unsubscribe(topic)

	sub.mu.Lock()
	if sub.SubscribedTopics[topic] {
		t.Fatal("Topic not unsubscribed")
	}
	sub.mu.Unlock()
}
