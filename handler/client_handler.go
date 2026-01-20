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
	subscriber := client.NewSubscriber(conn)

	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		line := sc.Text()
		cmd, args := parser.Parse(line)

		response := handleCommand(subscriber, broker, cmd, args)

		if _, err := conn.Write([]byte(response)); err != nil {
			subscriber.Active = false
			fmt.Println("Client disconnected: ", err)
			return
		}
	}

	subscriber.Active = false
	broker.UnsubscribeAll(subscriber)
}
