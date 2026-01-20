package main

import (
	"net"
	"pxmq/handler"
)

func main() {
	// broker := broker.NewBroker()
	// hub := client.NewClientsHub()

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

		go handler.HandleClient(tcpConn, nil)
	}
}
