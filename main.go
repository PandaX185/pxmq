package main

import (
	"net"
	"pxmq/broker"
	"pxmq/handler"
)

func main() {
	broker := broker.NewBroker()

	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	for {
		conn, err := srv.Accept()
		if err != nil {
			continue
		}

		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			conn.Close()
			continue
		}

		go handler.HandleClient(tcpConn, broker)
	}
}
