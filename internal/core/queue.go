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
	readyNotify  chan struct{}
}

func NewQueue(name string, isDurable bool, isAutoDelete bool, maxAttempts int, filters []RoutingFilter) *Queue {
	return &Queue{
		Name:         name,
		Filters:      filters,
		IsDurable:    isDurable,
		IsAutoDelete: isAutoDelete,
		MaxAttempts:  maxAttempts,
		InFlight:     make(map[string]Delivery),
		Consumers:    make(map[string]struct{}),
		readyNotify:  make(chan struct{}),
	}
}

func (q *Queue) Append(msg Message) Delivery {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := uuid.New().String()

	delivery := Delivery{ID: id, Message: msg}
	q.Ready = append(q.Ready, delivery)
	q.notifyReadyLocked()
	return delivery
}

func (q *Queue) RestoreReady(delivery Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delivery.ConsumerID = ""
	q.Ready = append(q.Ready, delivery)
	q.notifyReadyLocked()
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
			q.notifyReadyLocked()
			return delivery, false, nil
		}
	}

	return delivery, true, nil
}

func (q *Queue) DisconnectConsumer(consumerID string) []Delivery {
	q.mu.Lock()
	defer q.mu.Unlock()

	var returned []Delivery
	for _, delivery := range q.InFlight {
		if delivery.ConsumerID == consumerID {
			delete(q.InFlight, delivery.ID)
			delivery.ConsumerID = ""
			q.Ready = append(q.Ready, delivery)
			returned = append(returned, delivery)
		}
	}
	if len(returned) > 0 {
		q.notifyReadyLocked()
	}
	delete(q.Consumers, consumerID)

	return returned
}

func (q *Queue) AddConsumer(consumerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Consumers[consumerID] = struct{}{}
}
func (q *Queue) HasConsumers() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.Consumers) != 0
}
func (q *Queue) EnqueueDelivery(delivery Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Ready = append(q.Ready, delivery)
	q.notifyReadyLocked()
}

func (q *Queue) FetchOrWait(consumerID string, maxInFlight int) (Delivery, <-chan struct{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Consumers[consumerID]; !ok {
		return Delivery{}, nil, errs.ConsumerNotFound
	}

	if maxInFlight > 0 && q.consumerInFlightCountLocked(consumerID) >= maxInFlight {
		return Delivery{}, nil, errs.TooManyInFlight
	}

	if len(q.Ready) == 0 {
		return Delivery{}, q.readyNotify, errs.QueueEmpty
	}

	delivery := q.Ready[0]
	delivery.Attempts++
	delivery.ConsumerID = consumerID

	q.InFlight[delivery.ID] = delivery
	q.Ready = slices.Delete(q.Ready, 0, 1)

	return delivery, nil, nil
}

func (q *Queue) consumerInFlightCountLocked(consumerID string) int {
	count := 0
	for _, delivery := range q.InFlight {
		if delivery.ConsumerID == consumerID {
			count++
		}
	}

	return count
}

func (q *Queue) MatchFilters(key RoutingKey) bool {
	for _, v := range q.Filters {
		if v.Match(key) {
			return true
		}
	}
	return false
}

func (q *Queue) notifyReadyLocked() {
	close(q.readyNotify)
	q.readyNotify = make(chan struct{})
}
