package handler

import (
	"bufio"
	"net"
	"pxmq/broker"
	"pxmq/client"
	"pxmq/parser"
)

func HandleClient(conn *net.TCPConn, broker *broker.Broker, hub *client.ClientsHub) {
	client := hub.AddClient(conn)
	defer hub.RemoveClient(client.ID)

	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		cmd, args := parser.Parse(sc.Text())
		res := handleCommand(client, broker, cmd, args)

		conn.Write([]byte(res))
	}
}
