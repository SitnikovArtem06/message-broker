package core

import (
	"slices"
	"sync"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
	"github.com/google/uuid"
)

type Queue struct {
	mu           sync.Mutex
	Name         string
	Filters      []RoutingFilter
	IsDurable    bool
	IsAutoDelete bool
	MaxAttempts  int
	Ready        []Delivery
	InFlight     map[string]Delivery
	Consumers    map[string]struct{}
}

func NewQueue(name string, isDurable bool, isAutoDelete bool, filters []RoutingFilter) *Queue {
	return &Queue{
		Name:         name,
		Filters:      filters,
		IsDurable:    isDurable,
		IsAutoDelete: isAutoDelete,
		InFlight:     make(map[string]Delivery),
		Consumers:    make(map[string]struct{}),
	}
}

func (q *Queue) Fetch(consumerID string) (Delivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Consumers[consumerID]; !ok {
		return Delivery{}, errs.ConsumerNotFound
	}

	if len(q.Ready) == 0 {
		return Delivery{}, errs.QueueEmpty
	}

	delivery := q.Ready[0]
	delivery.Attempts++
	delivery.ConsumerID = consumerID

	q.InFlight[delivery.ID] = delivery
	q.Ready = slices.Delete(q.Ready, 0, 1)
	return delivery, nil
}

func (q *Queue) Append(msg Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := uuid.New().String()

	delivery := Delivery{ID: id, Message: msg}
	q.Ready = append(q.Ready, delivery)
}

func (q *Queue) Ack(deliveryID string, consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	delivery, ok := q.InFlight[deliveryID]
	if !ok {
		return errs.DeliveryNotFound
	}
	if delivery.ConsumerID != consumerID {
		return errs.DeliveryOwnerMismatch
	}

	delete(q.InFlight, deliveryID)
	return nil
}

func (q *Queue) Reject(deliveryID string, consumerID string) (Delivery, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delivery, ok := q.InFlight[deliveryID]
	if !ok {
		return Delivery{}, false, errs.DeliveryNotFound
	}
	if delivery.ConsumerID != consumerID {
		return Delivery{}, false, errs.DeliveryOwnerMismatch
	}

	delete(q.InFlight, deliveryID)

	if q.MaxAttempts > 0 && delivery.Attempts >= q.MaxAttempts {
		return delivery, true, nil
	}
	for consumerID := range q.Consumers {
		if consumerID != delivery.ConsumerID {
			delivery.ConsumerID = ""
			q.Ready = append(q.Ready, delivery)
			return Delivery{}, false, nil
		}
	}

	return delivery, true, nil
}

func (q *Queue) DisconnectConsumer(consumerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, delivery := range q.InFlight {
		if delivery.ConsumerID == consumerID {
			delete(q.InFlight, delivery.ID)
			delivery.ConsumerID = ""
			q.Ready = append(q.Ready, delivery)
		}
	}
	delete(q.Consumers, consumerID)
}

func (q *Queue) AddConsumer(consumerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Consumers[consumerID] = struct{}{}
}

func (q *Queue) RemoveConsumer(consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Consumers[consumerID]; !ok {
		return errs.ConsumerNotFound
	}

	delete(q.Consumers, consumerID)
	return nil
}

func (q *Queue) HasConsumers() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.Consumers) != 0
}

func (q *Queue) HasOtherConsumers(consumerID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for k := range q.Consumers {
		if k != consumerID {
			return true
		}
	}
	return false
}

func (q *Queue) MatchFilters(key RoutingKey) bool {
	for _, v := range q.Filters {
		if v.Match(key) {
			return true
		}
	}
	return false
}
