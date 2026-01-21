package handler

import (
	"bufio"
	"net"
	"pxmq/broker"
	"strings"
	"testing"
	"time"
)

func TestTCPCommunication(t *testing.T) {
	// Start broker
	b := broker.NewBroker()

	// Use net.Pipe for testing
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Start HandleClient in goroutine
	go HandleClient(serverConn, b)

	// Client sends PUBLISH
	_, err := clientConn.Write([]byte("pub test_topic test_message\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Read response
	scanner := bufio.NewScanner(clientConn)
	if !scanner.Scan() {
		t.Fatal("No response")
	}
	response := scanner.Text()
	if response != "OK" {
		t.Errorf("Expected OK, got %s", response)
	}

	// Another pipe for subscriber
	serverConn2, clientConn2 := net.Pipe()
	defer serverConn2.Close()
	defer clientConn2.Close()

	go HandleClient(serverConn2, b)

	// Send SUBSCRIBE
	_, err = clientConn2.Write([]byte("sub test_topic\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Read OK
	scanner2 := bufio.NewScanner(clientConn2)
	if !scanner2.Scan() {
		t.Fatal("No response")
	}
	response2 := scanner2.Text()
	if response2 != "OK" {
		t.Errorf("Expected OK, got %s", response2)
	}

	// Publish another message
	_, err = clientConn.Write([]byte("pub test_topic another_message\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Read OK
	if !scanner.Scan() {
		t.Fatal("No response")
	}
	response = scanner.Text()
	if response != "OK" {
		t.Errorf("Expected OK, got %s", response)
	}

	// Subscriber should receive message
	clientConn2.SetReadDeadline(time.Now().Add(1 * time.Second))
	if scanner2.Scan() {
		msg := scanner2.Text()
		expected := "MESSAGE test_topic another_message"
		if !strings.Contains(msg, expected) {
			t.Errorf("Expected message containing %s, got %s", expected, msg)
		}
	} else {
		t.Error("No message received by subscriber")
	}
}
