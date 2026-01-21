package handler

import (
	"bufio"
	"fmt"
	"net"
	"pxmq/broker"
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

	// Client sends PUBLISH with length-prefixed payload
	message := "test_message"
	payload := fmt.Sprintf("PUB test_topic %d\n%s", len(message), message)
	_, err := clientConn.Write([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}

	// Read response
	scanner := bufio.NewScanner(clientConn)
	if !scanner.Scan() {
		t.Fatal("No response")
	}
	response := scanner.Text()
	if response != "+OK" {
		t.Errorf("Expected +OK, got %s", response)
	}

	// Another pipe for subscriber
	serverConn2, clientConn2 := net.Pipe()
	defer serverConn2.Close()
	defer clientConn2.Close()

	go HandleClient(serverConn2, b)

	// Send SUBSCRIBE
	_, err = clientConn2.Write([]byte("SUB test_topic\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Read OK
	scanner2 := bufio.NewScanner(clientConn2)
	if !scanner2.Scan() {
		t.Fatal("No response")
	}
	response2 := scanner2.Text()
	if response2 != "+OK" {
		t.Errorf("Expected +OK, got %s", response2)
	}

	// Publish another message
	message2 := "another_message"
	payload2 := fmt.Sprintf("PUB test_topic %d\n%s", len(message2), message2)
	_, err = clientConn.Write([]byte(payload2))
	if err != nil {
		t.Fatal(err)
	}

	// Read OK
	if !scanner.Scan() {
		t.Fatal("No response")
	}
	response = scanner.Text()
	if response != "+OK" {
		t.Errorf("Expected +OK, got %s", response)
	}

	// Subscriber should receive message
	clientConn2.SetReadDeadline(time.Now().Add(1 * time.Second))
	if scanner2.Scan() {
		msgLine := scanner2.Text()
		expectedLine := fmt.Sprintf("MSG test_topic %d", len(message2))
		if msgLine != expectedLine {
			t.Errorf("Expected MSG line %q, got %q", expectedLine, msgLine)
		}
		// Read the payload
		if scanner2.Scan() {
			payload := scanner2.Text()
			if payload != message2 {
				t.Errorf("Expected payload %q, got %q", message2, payload)
			}
		} else {
			t.Error("No payload received")
		}
	} else {
		t.Error("No message received by subscriber")
	}
}
