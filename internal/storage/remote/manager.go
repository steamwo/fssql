package remote

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
)

var ErrClientLimit = errors.New("remote: client limit reached")

type ClientKey struct {
	Endpoint          string
	CredentialScope   string
	CredentialVersion string
	SecurityProfile   string
}

func (k ClientKey) Validate() error {
	if k.Endpoint == "" {
		return errors.New("remote: empty endpoint")
	}
	if k.CredentialScope == "" {
		return errors.New("remote: empty credential scope")
	}
	if k.SecurityProfile == "" {
		return errors.New("remote: empty security profile")
	}
	return nil
}

type ClientFactory func(ClientKey) (*http.Client, error)

type ClientManager struct {
	mu         sync.Mutex
	clients    map[ClientKey]*http.Client
	maxClients int
	factory    ClientFactory
}

func NewClientManager(maxClients int, factory ClientFactory) *ClientManager {
	if maxClients <= 0 {
		maxClients = 32
	}
	if factory == nil {
		factory = func(ClientKey) (*http.Client, error) { return &http.Client{}, nil }
	}
	return &ClientManager{clients: make(map[ClientKey]*http.Client), maxClients: maxClients, factory: factory}
}

func (m *ClientManager) Client(key ClientKey) (*http.Client, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[key]; ok {
		return c, nil
	}
	if len(m.clients) >= m.maxClients {
		return nil, fmt.Errorf("%w: max=%d", ErrClientLimit, m.maxClients)
	}
	c, err := m.factory(key)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("remote: factory returned nil client")
	}
	m.clients[key] = c
	return c, nil
}

func (m *ClientManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients)
}
