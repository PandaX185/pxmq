package handler

import (
	"fmt"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/message"
)

func handleCommand(c *client.Subscriber, b *broker.Broker, cmd string, args []string) string {
	switch cmd {
	case "pub":
		if len(args) < 1 {
			return "ERROR: PUBLISH requires queue name and message\n"
		}
		if len(args) < 2 {
			return "ERROR: Cannot publish empty message\n"
		}

		queueName := args[0]
		msg := []byte(args[1])
		t := b.GetOrCreateTopic(queueName)
		t.Publish(*message.NewMessage(msg))

		return "OK\n"
	case "sub":
		if len(args) < 1 {
			return "ERROR: SUBSCRIBE requires queue name\n"
		}

		replay := false
		topics := args
		if len(args) >= 2 {
			replay = args[len(args)-1] == "*"
			topics = args[:len(args)-1]
		}

		for _, t := range topics {
			topic := b.GetOrCreateTopic(t)
			topic.AddSubscriber(c)
			c.Subscribe(t)
			go topic.Consume(c, replay)
		}
		return "OK\n"
	case "unsub":
		if len(args) < 1 {
			return "ERROR: UNSUBSCRIBE requires queue name\n"
		}

		if t, exists := b.GetTopic(args[0]); exists {
			c.Unsubscribe(args[0])
			t.Unsubscribe(c)
		}
		return "OK\n"
	case "MESSAGE":
		return ""
	default:
		return fmt.Sprintf("UNKNOWN COMMAND: %s\n", cmd)
	}
}
