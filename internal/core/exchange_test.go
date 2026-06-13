package core

import (
	"errors"
	"testing"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

func newTestExchange(t *testing.T) *Exchange {
	t.Helper()

	broker := &Broker{
		Exchanges: make(map[string]*Exchange),
	}

	return broker.CreateExchange("corp")
}

func registerTestQueue(t *testing.T, exchange *Exchange, name string, filters []RoutingFilter) *Queue {
	t.Helper()

	queue, err := exchange.RegisterQueue(name, false, false, filters)
	if err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}

	return queue
}

func TestDeliveryPublishFetchAck(t *testing.T) {
	exchange := newTestExchange(t)
	queue := registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.AddConsumer("users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := exchange.Fetch("users", "consumer-1")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
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

func TestDeliveryFetchRequiresRegisteredConsumer(t *testing.T) {
	exchange := newTestExchange(t)
	registerTestQueue(t, exchange, "users", []RoutingFilter{"corp.users.*"})

	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	_, err := exchange.Fetch("users", "missing-consumer")
	if !errors.Is(err, errs.ConsumerNotFound) {
		t.Fatalf("Fetch() error = %v, want %v", err, errs.ConsumerNotFound)
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
	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := exchange.Fetch("users", "consumer-1")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	err = exchange.Ack("users", delivery.ID, "consumer-2")
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
	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := exchange.Fetch("users", "consumer-1")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if err := exchange.NAck("users", delivery.ID, "consumer-1"); err != nil {
		t.Fatalf("NAck() error = %v", err)
	}

	if len(exchange.DeadLetters.Items) != 0 {
		t.Fatalf("dead-letter count = %d, want 0", len(exchange.DeadLetters.Items))
	}
	if len(queue.Ready) != 1 {
		t.Fatalf("ready count = %d, want 1", len(queue.Ready))
	}

	redelivery, err := exchange.Fetch("users", "consumer-2")
	if err != nil {
		t.Fatalf("second Fetch() error = %v", err)
	}
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
	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := exchange.Fetch("users", "consumer-1")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if err := exchange.NAck("users", delivery.ID, "consumer-1"); err != nil {
		t.Fatalf("NAck() error = %v", err)
	}

	if len(queue.Ready) != 0 {
		t.Fatalf("ready count = %d, want 0", len(queue.Ready))
	}
	if len(queue.InFlight) != 0 {
		t.Fatalf("in-flight count = %d, want 0", len(queue.InFlight))
	}
	if len(exchange.DeadLetters.Items) != 1 {
		t.Fatalf("dead-letter count = %d, want 1", len(exchange.DeadLetters.Items))
	}

	letter := exchange.DeadLetters.Items[0]
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
	if err := exchange.Publish("corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if _, err := exchange.Fetch("users", "consumer-1"); err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	if err := exchange.DisconnectConsumer("users", "consumer-1"); err != nil {
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

	if _, err := exchange.RegisterQueue("temp", false, true, []RoutingFilter{"corp.temp.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := exchange.AddConsumer("temp", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	if err := exchange.DisconnectConsumer("temp", "consumer-1"); err != nil {
		t.Fatalf("DisconnectConsumer() error = %v", err)
	}
	if _, ok := exchange.Queues["temp"]; ok {
		t.Fatal("auto-delete queue still exists after last consumer disconnect")
	}
}
