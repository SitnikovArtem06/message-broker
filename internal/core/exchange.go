package core

import (
	"sync"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

type Exchange struct {
	mu     sync.RWMutex
	Name   string
	Queues map[string]*Queue
}

func (e *Exchange) RegisterQueue(name string, IsDurable bool, IsAutoDelete bool, maxAttempts int, filters []RoutingFilter) (*Queue, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if queue, ok := e.Queues[name]; ok {
		if queue.IsDurable == IsDurable && queue.IsAutoDelete == IsAutoDelete && queue.MaxAttempts == maxAttempts && equalFilters(queue.Filters, filters) {
			return queue, nil
		}
		return nil, errs.QueueAlreadyExist
	}

	if IsDurable && IsAutoDelete {
		return nil, errs.QueueFlagsConflict
	}

	if maxAttempts < 0 {
		return nil, errs.MaxAttemptsIncorrect
	}

	if !validFilters(filters) {
		return nil, errs.FiltersIncorrect
	}

	queue := NewQueue(name, IsDurable, IsAutoDelete, maxAttempts, filters)

	e.Queues[name] = queue

	return queue, nil
}

func (e *Exchange) DeleteQueue(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	queue, ok := e.Queues[name]
	if !ok {
		return errs.QueueNotFound
	}
	if queue.HasConsumers() {
		return errs.QueueHasConsumers
	}

	delete(e.Queues, name)
	return nil
}

func (e *Exchange) GetQueue(name string) (*Queue, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	queue, ok := e.Queues[name]
	if !ok {
		return nil, errs.QueueNotFound
	}

	return queue, nil
}
func (e *Exchange) Publish(routingKey string, payload []byte) ([]PublishedDelivery, error) {
	published, err := e.Route(routingKey, payload)
	if err != nil {
		return nil, err
	}

	for i := range published {
		delivery := published[i].Queue.Append(published[i].Message)
		published[i].Delivery = delivery
	}

	return published, nil
}

func (e *Exchange) Route(routingKey string, payload []byte) ([]PublishedDelivery, error) {
	key := RoutingKey(routingKey)
	if !key.IsValid() {
		return nil, errs.InvalidRoutingKey
	}

	e.mu.RLock()
	queues := make([]*Queue, 0, len(e.Queues))
	for _, q := range e.Queues {
		queues = append(queues, q)
	}
	e.mu.RUnlock()

	var published []PublishedDelivery
	msg := Message{RoutingKey: key, Payload: payload}
	for _, q := range queues {
		if q.MatchFilters(key) {
			published = append(published, PublishedDelivery{QueueName: q.Name, Queue: q, Message: msg})
		}
	}

	return published, nil
}

func (e *Exchange) EnqueueDelivery(queueName string, delivery Delivery) error {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return err
	}
	queue.EnqueueDelivery(delivery)
	return nil
}

func (e *Exchange) FetchOrWait(queueName, consumerID string, maxInFlight int) (Delivery, <-chan struct{}, error) {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return Delivery{}, nil, err
	}

	if delivery, waitCh, err := queue.FetchOrWait(consumerID, maxInFlight); err != nil {
		return Delivery{}, waitCh, err
	} else {
		return delivery, nil, nil
	}

}

func (e *Exchange) Ack(queueName, deliveryID, consumerID string) error {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return err
	}

	if err := queue.Ack(deliveryID, consumerID); err != nil {
		return err
	}

	return nil
}

type NAckResult struct {
	Delivery   Delivery
	DeadLetter DeadLetter
	IsDead     bool
}

func (e *Exchange) NAck(queueName, deliveryID, consumerID string) (NAckResult, error) {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return NAckResult{}, err
	}

	delivery, shouldDeadLetter, err := queue.Reject(deliveryID, consumerID)
	if err != nil {
		return NAckResult{}, err
	}

	var deadLetter DeadLetter

	if shouldDeadLetter {
		deadLetter = DeadLetter{
			Message:     delivery.Message,
			SourceQueue: queueName,
			Reason:      "nack without another consumer",
			Attempts:    delivery.Attempts,
		}
	}

	return NAckResult{Delivery: delivery, DeadLetter: deadLetter, IsDead: shouldDeadLetter}, nil
}

func (e *Exchange) AddConsumer(queueName, consumerID string) error {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return err
	}

	queue.AddConsumer(consumerID)
	return nil
}

func (e *Exchange) DisconnectConsumer(queueName, consumerID string) ([]Delivery, error) {
	queue, err := e.GetQueue(queueName)
	if err != nil {
		return nil, err
	}

	returned := queue.DisconnectConsumer(consumerID)

	if queue.IsAutoDelete && !queue.HasConsumers() {
		e.mu.Lock()
		delete(e.Queues, queueName)
		e.mu.Unlock()
	}
	return returned, nil
}

func (e *Exchange) HasActiveConsumers() bool {
	e.mu.RLock()
	queues := make([]*Queue, 0, len(e.Queues))
	for _, queue := range e.Queues {
		queues = append(queues, queue)
	}
	e.mu.RUnlock()

	for _, queue := range queues {
		if queue.HasConsumers() {
			return true
		}
	}

	return false
}

func equalFilters(a, b []RoutingFilter) bool {
	if len(a) != len(b) {
		return false
	}
	mapA := make(map[RoutingFilter]struct{})
	mapB := make(map[RoutingFilter]struct{})

	for i := 0; i < len(a); i++ {
		mapA[a[i]] = struct{}{}
		mapB[b[i]] = struct{}{}
	}

	for v := range mapA {
		if _, ok := mapB[v]; !ok {
			return false
		}
	}

	return true
}

func validFilters(filters []RoutingFilter) bool {
	for _, v := range filters {
		if !v.IsValid() {
			return false
		}
	}
	return true
}
