package core

import (
	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

type Exchange struct {
	Name        string
	Queues      map[string]*Queue
	DeadLetters *DeadLetterQueue
}

func (e *Exchange) RegisterQueue(name string, IsDurable bool, IsAutoDelete bool, filters []RoutingFilter) (*Queue, error) {
	if queue, ok := e.Queues[name]; ok {
		if queue.IsDurable == IsDurable && queue.IsAutoDelete == IsAutoDelete && equalFilters(queue.Filters, filters) {
			return queue, nil
		}
		return nil, errs.QueueAlreadyExist
	}

	if IsDurable && IsAutoDelete {
		return nil, errs.QueueFlagsConflict
	}

	if !validFilters(filters) {
		return nil, errs.FiltersIncorrect
	}

	queue := NewQueue(name, IsDurable, IsAutoDelete, filters)

	e.Queues[name] = queue

	return queue, nil
}

func (e *Exchange) DeleteQueue(name string) error {
	if _, ok := e.Queues[name]; !ok {
		return errs.QueueNotFound
	}

	delete(e.Queues, name)
	return nil
}

func validFilters(filters []RoutingFilter) bool {
	for _, v := range filters {
		if !v.IsValid() {
			return false
		}
	}
	return true
}

func (e *Exchange) Publish(routingKey string, payload []byte) error {
	key := RoutingKey(routingKey)
	if !key.IsValid() {
		return errs.InvalidRoutingKey
	}

	msg := Message{RoutingKey: key, Payload: payload}
	for _, q := range e.Queues {
		if q.MatchFilters(key) {
			q.Append(msg)
		}
	}

	return nil
}

func (e *Exchange) Fetch(queueName, consumerID string) (Delivery, error) {
	queue, ok := e.Queues[queueName]
	if !ok {
		return Delivery{}, errs.QueueNotFound
	}

	if delivery, err := queue.Fetch(consumerID); err != nil {
		return Delivery{}, err
	} else {
		return delivery, nil
	}

}

func (e *Exchange) Ack(queueName, deliveryID, consumerID string) error {
	queue, ok := e.Queues[queueName]
	if !ok {
		return errs.QueueNotFound
	}

	if err := queue.Ack(deliveryID, consumerID); err != nil {
		return err
	}

	return nil
}

func (e *Exchange) NAck(queueName, deliveryID, consumerID string) error {
	queue, ok := e.Queues[queueName]
	if !ok {
		return errs.QueueNotFound
	}

	delivery, shouldDeadLetter, err := queue.Reject(deliveryID, consumerID)
	if err != nil {
		return err
	}

	if shouldDeadLetter {
		e.DeadLetters.Append(DeadLetter{
			Message:     delivery.Message,
			SourceQueue: queueName,
			Reason:      "nack without another consumer",
			Attempts:    delivery.Attempts,
		})
	}

	return nil
}

func (e *Exchange) AddConsumer(queueName, consumerID string) error {
	queue, ok := e.Queues[queueName]
	if !ok {
		return errs.QueueNotFound
	}

	queue.AddConsumer(consumerID)
	return nil
}

func (e *Exchange) DisconnectConsumer(queueName, consumerID string) error {
	queue, ok := e.Queues[queueName]
	if !ok {
		return errs.QueueNotFound
	}

	queue.DisconnectConsumer(consumerID)

	if queue.IsAutoDelete && !queue.HasConsumers() {
		delete(e.Queues, queueName)
	}
	return nil
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
