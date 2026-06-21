package core

import (
	"errors"
	"testing"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

func newTestExchange(t *testing.T) *Exchange {
	t.Helper()

	broker := NewBroker()

	return broker.CreateExchange("corp")
}

func registerTestQueue(t *testing.T, exchange *Exchange, name string, filters []RoutingFilter) *Queue {
	t.Helper()

	queue, err := exchange.RegisterQueue(name, false, false, 0, filters)
	if err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}

	return queue
}

func fetchTestDelivery(t *testing.T, exchange *Exchange, queueName string, consumerID string) Delivery {
	t.Helper()

	delivery, _, err := exchange.FetchOrWait(queueName, consumerID, 0)
	if err != nil {
		t.Fatalf("FetchOrWait() error = %v", err)
	}

	return delivery
}

func TestDeliveryPublishFetchAck(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery := fetchTestDelivery(t, exchange, "users", "consumer-1")
	if string(delivery.Message.Payload) != "payload" {
		t.Fatalf("payload = %q, want %q", delivery.Message.Payload, "payload")
	}
	if delivery.ConsumerID != "consumer-1" {
		t.Fatalf("consumer id = %q, want %q", delivery.ConsumerID, "consumer-1")
	}

	if err := exchange.Ack("users", delivery.ID, "consumer-1"); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if len(queue.InFlight) != 0 {
		t.Fatalf("in-flight count = %d, want 0", len(queue.InFlight))
	}
}

func TestRegisterQueueIsIdempotentWithSameParameters(t *testing.T) {
	exchange := newTestExchange(t)

	first, err := exchange.RegisterQueue("users", true, false, 3, []RoutingFilter{"corp.users.create", "corp.users.*"})
	if err != nil {
		t.Fatalf("first RegisterQueue() error = %v", err)
	}

	second, err := exchange.RegisterQueue("users", true, false, 3, []RoutingFilter{"corp.users.*", "corp.users.create"})
	if err != nil {
		t.Fatalf("second RegisterQueue() error = %v", err)
	}

	if first != second {
		t.Fatal("RegisterQueue() returned different queues for the same parameters")
	}
}

func TestRegisterQueueFailsOnConflictingParameters(t *testing.T) {
	exchange := newTestExchange(t)

	if _, err := exchange.RegisterQueue("users", false, false, 0, []RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("first RegisterQueue() error = %v", err)
	}

	_, err := exchange.RegisterQueue("users", true, false, 0, []RoutingFilter{"corp.users.*"})
	if !errors.Is(err, errs.QueueAlreadyExist) {
		t.Fatalf("second RegisterQueue() error = %v, want %v", err, errs.QueueAlreadyExist)
	}
}

func TestPublishWithoutMatchingQueuesDropsMessage(t *testing.T) {
	exchange := newTestExchange(t)
	registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	published, err := exchange.Publish("billing.invoice.created", []byte("payload"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(published) != 0 {
		t.Fatalf("published count = %d, want 0", len(published))
	}
}

func TestPublishRoutesToSingleQueue(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	published, err := exchange.Publish("corp.users.create", []byte("payload"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("published count = %d, want 1", len(published))
	}
	if len(queue.Ready) != 1 {
		t.Fatalf("ready count = %d, want 1", len(queue.Ready))
	}
}

func TestPublishRoutesToMultipleQueues(t *testing.T) {
	exchange := newTestExchange(t)
	queueUsers := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})
	queueAudit := registerTestQueue(t, exchange, "audit", []RoutingFilter{"corp.*.create"})

	published, err := exchange.Publish("corp.users.create", []byte("payload"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("published count = %d, want 2", len(published))
	}
	if len(queueUsers.Ready) != 1 {
		t.Fatalf("users ready count = %d, want 1", len(queueUsers.Ready))
	}
	if len(queueAudit.Ready) != 1 {
		t.Fatalf("audit ready count = %d, want 1", len(queueAudit.Ready))
	}
}

func TestDeliveryNAckDeadLettersWhenMaxAttemptsReached(t *testing.T) {
	exchange := newTestExchange(t)
	queue, err := exchange.RegisterQueue("users", false, false, 1, []RoutingFilter{"corp.users.*"})
	if err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery := fetchTestDelivery(t, exchange, "users", "consumer-1")
	result, err := exchange.NAck("users", delivery.ID, "consumer-1")
	if err != nil {
		t.Fatalf("NAck() error = %v", err)
	}

	if !result.IsDead {
		t.Fatal("NAck() did not send delivery to dead-letter after max attempts")
	}
	if result.DeadLetter.Attempts != 1 {
		t.Fatalf("dead-letter attempts = %d, want 1", result.DeadLetter.Attempts)
	}
	if len(queue.Ready) != 0 {
		t.Fatalf("ready count = %d, want 0", len(queue.Ready))
	}
}

func TestDeliveryFetchRequiresRegisteredConsumer(t *testing.T) {
	exchange := newTestExchange(t)
	registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	_, _, err := exchange.FetchOrWait("users", "missing-consumer", 0)
	if !errors.Is(err, errs.ConsumerNotFound) {
		t.Fatalf("FetchOrWait() error = %v, want %v", err, errs.ConsumerNotFound)
	}
}

func TestDeliveryFetchRespectsMaxInFlightPerConsumer(t *testing.T) {
	exchange := newTestExchange(t)
	registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload-1")); err != nil {
		t.Fatalf("Publish(payload-1) error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.update", []byte("payload-2")); err != nil {
		t.Fatalf("Publish(payload-2) error = %v", err)
	}

	if _, _, err := exchange.FetchOrWait("users", "consumer-1", 1); err != nil {
		t.Fatalf("first FetchOrWait() error = %v", err)
	}

	_, _, err := exchange.FetchOrWait("users", "consumer-1", 1)
	if !errors.Is(err, errs.TooManyInFlight) {
		t.Fatalf("second FetchOrWait() error = %v, want %v", err, errs.TooManyInFlight)
	}
}

func TestDeliveryAckRejectsAnotherConsumer(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer(consumer-1) error = %v", err)
	}
	if err := exchange.AddConsumer("users", "consumer-2"); err != nil {
		t.Fatalf("AddConsumer(consumer-2) error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery := fetchTestDelivery(t, exchange, "users", "consumer-1")

	err := exchange.Ack("users", delivery.ID, "consumer-2")
	if !errors.Is(err, errs.DeliveryOwnerMismatch) {
		t.Fatalf("Ack() error = %v, want %v", err, errs.DeliveryOwnerMismatch)
	}
	if len(queue.InFlight) != 1 {
		t.Fatalf("in-flight count = %d, want 1", len(queue.InFlight))
	}
}

func TestDeliveryNAckRequeuesWhenAnotherConsumerExists(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer(consumer-1) error = %v", err)
	}
	if err := exchange.AddConsumer("users", "consumer-2"); err != nil {
		t.Fatalf("AddConsumer(consumer-2) error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery := fetchTestDelivery(t, exchange, "users", "consumer-1")
	result, err := exchange.NAck("users", delivery.ID, "consumer-1")
	if err != nil {
		t.Fatalf("NAck() error = %v", err)
	}

	if result.IsDead {
		t.Fatal("NAck() sent delivery to dead-letter, want requeue")
	}
	if len(queue.Ready) != 1 {
		t.Fatalf("ready count = %d, want 1", len(queue.Ready))
	}

	redelivery := fetchTestDelivery(t, exchange, "users", "consumer-2")
	if redelivery.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", redelivery.Attempts)
	}
}

func TestDeliveryNAckDeadLettersWithoutAnotherConsumer(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery := fetchTestDelivery(t, exchange, "users", "consumer-1")
	result, err := exchange.NAck("users", delivery.ID, "consumer-1")
	if err != nil {
		t.Fatalf("NAck() error = %v", err)
	}

	if len(queue.Ready) != 0 {
		t.Fatalf("ready count = %d, want 0", len(queue.Ready))
	}
	if len(queue.InFlight) != 0 {
		t.Fatalf("in-flight count = %d, want 0", len(queue.InFlight))
	}
	if !result.IsDead {
		t.Fatal("NAck() did not send delivery to dead-letter")
	}

	letter := result.DeadLetter
	if letter.SourceQueue != "users" {
		t.Fatalf("source queue = %q, want %q", letter.SourceQueue, "users")
	}
	if string(letter.Message.Payload) != "payload" {
		t.Fatalf("dead-letter payload = %q, want %q", letter.Message.Payload, "payload")
	}
}

func TestDeliveryDisconnectReturnsInFlightToReady(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer(consumer-1) error = %v", err)
	}
	if _, err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	_ = fetchTestDelivery(t, exchange, "users", "consumer-1")

	if _, err := exchange.DisconnectConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("DisconnectConsumer() error = %v", err)
	}

	if len(queue.InFlight) != 0 {
		t.Fatalf("in-flight count = %d, want 0", len(queue.InFlight))
	}
	if len(queue.Ready) != 1 {
		t.Fatalf("ready count = %d, want 1", len(queue.Ready))
	}
}

func TestDeliveryAutoDeleteRemovesQueueAfterLastConsumerDisconnect(t *testing.T) {
	exchange := newTestExchange(t)

	if _, err := exchange.RegisterQueue("temp", false, true, 0, []RoutingFilter{"corp.temp.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := exchange.AddConsumer("temp", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	if _, err := exchange.DisconnectConsumer("temp", "consumer-1"); err != nil {
		t.Fatalf("DisconnectConsumer() error = %v", err)
	}
	if _, ok := exchange.Queues["temp"]; ok {
		t.Fatal("auto-delete queue still exists after last consumer disconnect")
	}
}

func TestDeleteQueueFailsWhenQueueHasActiveConsumers(t *testing.T) {
	exchange := newTestExchange(t)

	if _, err := exchange.RegisterQueue("users", false, false, 0, []RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err := exchange.DeleteQueue("users")
	if !errors.Is(err, errs.QueueHasConsumers) {
		t.Fatalf("DeleteQueue() error = %v, want %v", err, errs.QueueHasConsumers)
	}
}

func TestExchangeHasActiveConsumers(t *testing.T) {
	exchange := newTestExchange(t)

	if _, err := exchange.RegisterQueue("users", false, false, 0, []RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if exchange.HasActiveConsumers() {
		t.Fatal("HasActiveConsumers() = true, want false")
	}

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if !exchange.HasActiveConsumers() {
		t.Fatal("HasActiveConsumers() = false, want true")
	}
}
