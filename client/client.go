package client

import (
	"net"

	"github.com/gofrs/uuid"
)

type Client struct {
	ID               string
	Conn             *net.TCPConn
	SubscribedQueues []string
}

func NewClient(conn *net.TCPConn) *Client {
	id, err := uuid.NewV4()
	if err != nil {
		id = uuid.Must(uuid.NewV4())
	}
	return &Client{
		ID:               id.String(),
		Conn:             conn,
		SubscribedQueues: []string{},
	}
}

func (c *Client) Subscribe(queueNames ...string) {
	c.SubscribedQueues = append(c.SubscribedQueues, queueNames...)
}

func (c *Client) Unsubscribe(queueName string) {
	for i, q := range c.SubscribedQueues {
		if q == queueName {
			c.SubscribedQueues = append(c.SubscribedQueues[:i], c.SubscribedQueues[i+1:]...)
			break
		}
	}
}
