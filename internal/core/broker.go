package core

import errs "github.com/SitnikovArtem06/message-broker/internal/errors"

type Broker struct {
	Exchanges map[string]*Exchange
}

func (b *Broker) CreateExchange(name string) (*Exchange, error) {
	if _, ok := b.Exchanges[name]; ok {
		return nil, errs.ExchangeAlreadyExist
	}

	exchange := &Exchange{
		Name:   name,
		Queues: make(map[string]*Queue),
	}

	b.Exchanges[name] = exchange

	return exchange, nil

}

func (b *Broker) DeleteExchange(name string) error {
	if _, ok := b.Exchanges[name]; !ok {
		return errs.ExchangeNotFound
	}

	delete(b.Exchanges, name)
	return nil
}
