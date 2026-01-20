package client

import "sync"

type ClientsHub struct {
	clients map[int]*Client
	mu      sync.RWMutex
}

func (hub *ClientsHub) AddClient(client *Client) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.clients[client.ID] = client
}

func (hub *ClientsHub) RemoveClient(id int) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	delete(hub.clients, id)
}

func (hub *ClientsHub) GetClient(id int) *Client {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return hub.clients[id]
}
