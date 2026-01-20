package handler

import (
	"bufio"
	"fmt"
	"net"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/parser"
)

func HandleClient(conn *net.TCPConn, broker *broker.Broker) {
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		line := sc.Text()
		cmd, args := parser.Parse(line)
		subscriber := client.NewSubscriber(conn)
		response := handleCommand(subscriber, broker, cmd, args)
		if _, err := conn.Write([]byte(response)); err != nil {
			subscriber.Active = false
			fmt.Println("Client disconnected: ", err)
			return
		}
	}
}
