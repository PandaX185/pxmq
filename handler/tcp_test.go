package handler

import (
	"bufio"
	"fmt"
	"net"
	"pxmq/broker"
	"testing"
)

func TestTCPCommunication(t *testing.T) {
	// Create test data directory
	dataDir := t.TempDir()

	// Start broker
	b := broker.NewBroker(dataDir)

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

	b.Close()
}
