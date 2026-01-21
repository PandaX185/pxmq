package handler

import (
	"fmt"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/message"
	"pxmq/parser"
)

func handleCommand(c *client.Subscriber, b *broker.Broker, cmd *parser.Command) string {
	switch cmd.Type {
	case "PUB":
		if len(cmd.Args) < 1 {
			return "-ERR PUBLISH requires topic\n"
		}
		if len(cmd.Payload) == 0 {
			return "-ERR Cannot publish empty message\n"
		}

		topicName := cmd.Args[0]
		t := b.GetOrCreateTopic(topicName)
		t.Publish(*message.NewMessage(cmd.Payload))

		return "+OK\n"
	case "SUB":
		if len(cmd.Args) < 1 {
			return "-ERR SUBSCRIBE requires topics\n"
		}

		replay := false
		topics := cmd.Args
		if len(cmd.Args) >= 1 && cmd.Args[len(cmd.Args)-1] == "*" {
			replay = true
			topics = cmd.Args[:len(cmd.Args)-1]
		}

		for _, topicName := range topics {
			topic := b.GetOrCreateTopic(topicName)
			topic.AddSubscriber(c)
			c.Subscribe(topicName)
			go topic.Consume(c, replay)
		}
		return "+OK\n"
	case "UNSUB":
		if len(cmd.Args) < 1 {
			return "-ERR UNSUBSCRIBE requires topics\n"
		}

		for _, topicName := range cmd.Args {
			if t, exists := b.GetTopic(topicName); exists {
				c.Unsubscribe(topicName)
				t.Unsubscribe(c)
			}
		}
		return "+OK\n"
	default:
		return fmt.Sprintf("-ERR Unknown command: %s\n", cmd.Type)
	}
}
