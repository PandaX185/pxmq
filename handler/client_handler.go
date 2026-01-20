package handler

import (
	"bufio"
	"net"
	"pxmq/broker"
	"strings"
)

func HandleClient(conn *net.TCPConn, broker *broker.Broker) {
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		line := strings.Split(sc.Text(), " ")

		res := handleCommand(line[0], line[1:])

		conn.Write([]byte(res))
	}
}
