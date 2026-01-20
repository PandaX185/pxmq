package client

import (
	"net"
)

type Client struct {
	ID               int
	Conn             *net.TCPConn
	SubscribedQueues []string
}

func NewClient(id int, conn *net.TCPConn) *Client {
	return &Client{
		ID:               id,
		Conn:             conn,
		SubscribedQueues: []string{},
	}
}

func (c *Client) Subscribe(queueName string) {
	c.SubscribedQueues = append(c.SubscribedQueues, queueName)
}

func (c *Client) Unsubscribe(queueName string) {
	for i, q := range c.SubscribedQueues {
		if q == queueName {
			c.SubscribedQueues = append(c.SubscribedQueues[:i], c.SubscribedQueues[i+1:]...)
			break
		}
	}
}
