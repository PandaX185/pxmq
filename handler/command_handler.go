package handler

import (
	"fmt"
	"pxmq/broker"
	"pxmq/client"
)

func handleCommand(c *client.Client, b *broker.Broker, cmd string, args []string) string {
	switch cmd {
	case "PUBLISH":
		if len(args) < 1 {
			return "ERROR: PUBLISH requires queue name and message\n"
		}
		if len(args) < 2 {
			return "ERROR: Cannot publish empty message\n"
		}

		queueName := args[0]
		message := []byte(args[1])
		q := b.CreateInMemoryQueue(queueName)
		err := q.Enqueue(message)
		if err != nil {
			return fmt.Sprintf("ERROR: %v\n", err)
		}
		return "OK\n"
	case "SUBSCRIBE":
		if len(args) < 1 {
			return "ERROR: SUBSCRIBE requires queue name\n"
		}
		queueNames := args
		c.Subscribe(queueNames...)
		for _, queueName := range queueNames {
			go func(queueName string) {
				q, exists := b.GetQueue(queueName)
				if !exists {
					return
				}
				for {
					msg, err := q.Dequeue()
					if err != nil {
						break
					}
					c.Conn.Write([]byte(fmt.Sprintf("MESSAGE %s %s\n", queueName, string(msg))))
				}
			}(queueName)
		}
		return "OK\n"
	case "UNSUBSCRIBE":
		if len(args) < 1 {
			return "ERROR: UNSUBSCRIBE requires queue name\n"
		}
		queueName := args[0]
		c.Unsubscribe(queueName)
		return "OK\n"
	default:
		return fmt.Sprintf("UNKNOWN COMMAND: %s\n", cmd)
	}
}
