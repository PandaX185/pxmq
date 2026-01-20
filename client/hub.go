package client

import (
	"net"
	"sync"
)

type ClientsHub struct {
	clients map[string]*Client
	mu      sync.RWMutex
}

func NewClientsHub() *ClientsHub {
	return &ClientsHub{
		clients: make(map[string]*Client),
	}
}

func (hub *ClientsHub) AddClient(conn *net.TCPConn) *Client {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	client := NewClient(conn)
	hub.clients[client.ID] = client
	return client
}

func (hub *ClientsHub) RemoveClient(id string) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	delete(hub.clients, id)
}

func (hub *ClientsHub) GetClient(id string) *Client {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return hub.clients[id]
}
