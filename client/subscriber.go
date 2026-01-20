package client

import (
	"net"
)

type Subscriber struct {
	Conn   *net.TCPConn
	Offset int
	Active bool
}

func NewSubscriber(conn *net.TCPConn) *Subscriber {
	return &Subscriber{
		Conn:   conn,
		Offset: 0,
		Active: true,
	}
}
