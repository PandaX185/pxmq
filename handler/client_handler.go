package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/parser"
	"strings"
)

func HandleClient(conn net.Conn, broker *broker.Broker) {
	subscriber := client.NewSubscriber(conn)
	defer func() {
		subscriber.Active.Store(false)
		broker.UnsubscribeAll(subscriber)
		close(subscriber.MessageCh)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	for subscriber.Active.Load() {
		cmd, err := parser.ParseCommand(reader)
		if err != nil {
			if err == io.EOF {
				if len(subscriber.SubscribedTopics) == 0 {
					fmt.Println("Client disconnected")
					return
				}

				continue
			}
			if err.Error() == "empty line" {
				continue
			}
			fmt.Printf("Parse error: %v\n", err)
			response := fmt.Sprintf("-ERR Parse error: %v\n", err)
			_, err := conn.Write([]byte(response))
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				fmt.Printf("Write error: %v\n", err)
				return
			}
			continue
		}

		response := handleCommand(subscriber, broker, cmd)

		if _, err := conn.Write([]byte(response)); err != nil {
			subscriber.Active.Store(false)
			fmt.Println("Client disconnected: ", err)
			return
		}
	}
}
