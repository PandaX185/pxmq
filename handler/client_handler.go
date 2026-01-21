package handler

import (
	"bufio"
	"fmt"
	"net"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/parser"
)

func HandleClient(conn net.Conn, broker *broker.Broker) {
	subscriber := client.NewSubscriber(conn)
	defer func() {
		subscriber.Active.Store(false)
		broker.UnsubscribeAll(subscriber)
		close(subscriber.WriteCh)
		conn.Close()
	}()

	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		line := sc.Text()
		cmd, args := parser.Parse(line)

		response := handleCommand(subscriber, broker, cmd, args)

		if _, err := conn.Write([]byte(response)); err != nil {
			subscriber.Active.Store(false)
			fmt.Println("Client disconnected: ", err)
			return
		}
	}
}
