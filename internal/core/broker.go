package core

import errs "github.com/SitnikovArtem06/message-broker/internal/errors"

type Broker struct {
	Exchanges map[string]*Exchange
}

func (b *Broker) CreateExchange(name string) *Exchange {
	if exchange, ok := b.Exchanges[name]; ok {
		return exchange
	}

	exchange := &Exchange{
		Name:        name,
		Queues:      make(map[string]*Queue),
		DeadLetters: &DeadLetterQueue{},
	}

	b.Exchanges[name] = exchange

	return exchange

}

func (b *Broker) DeleteExchange(name string) error {
	if _, ok := b.Exchanges[name]; !ok {
		return errs.ExchangeNotFound
	}

	delete(b.Exchanges, name)
	return nil
}
