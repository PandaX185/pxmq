package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"pxmq/broker"
	"pxmq/handler"
)

func main() {
	port := flag.String("port", "8888", "Port to listen on")
	dataDir := flag.String("data", ".data", "Directory to store WAL files")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create data directory: %v", err))
	}

	broker := broker.NewBroker(*dataDir)

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
