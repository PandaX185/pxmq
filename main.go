package main

import (
	"flag"
	"fmt"
	"net"
	"pxmq/broker"
	"pxmq/handler"
)

func main() {
	port := flag.String("port", "8888", "Port to listen on")
	flag.Parse()

	broker := broker.NewBroker()

	addr := fmt.Sprintf(":%s", *port)
	srv, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	fmt.Printf("pxmq server listening on port %s\n", *port)

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
