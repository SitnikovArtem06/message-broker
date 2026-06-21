package core

import (
	"sync"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

type Broker struct {
	mu        sync.RWMutex
	Exchanges map[string]*Exchange
}

func NewBroker() *Broker {
	return &Broker{
		Exchanges: make(map[string]*Exchange),
	}
}

func (b *Broker) CreateExchange(name string) *Exchange {
	b.mu.Lock()
	defer b.mu.Unlock()

	if exchange, ok := b.Exchanges[name]; ok {
		return exchange
	}

	exchange := &Exchange{
		Name:   name,
		Queues: make(map[string]*Queue),
	}

	b.Exchanges[name] = exchange

	return exchange

}

func (b *Broker) GetExchange(name string) (*Exchange, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exchange, ok := b.Exchanges[name]
	if !ok {
		return nil, errs.ExchangeNotFound
	}

	return exchange, nil
}

func (b *Broker) DeleteExchange(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.Exchanges[name]; !ok {
		return errs.ExchangeNotFound
	}

	delete(b.Exchanges, name)
	return nil
}
