package core

import "testing"

func TestCreateExchange(t *testing.T) {
	broker := NewBroker()

	exchange := broker.CreateExchange("corp")
	if exchange == nil {
		t.Fatal("CreateExchange() returned nil exchange")
	}
	if exchange.Name != "corp" {
		t.Fatalf("exchange name = %q, want %q", exchange.Name, "corp")
	}
	if len(broker.Exchanges) != 1 {
		t.Fatalf("exchange count = %d, want 1", len(broker.Exchanges))
	}
}

func TestCreateExchangeIsIdempotent(t *testing.T) {
	broker := NewBroker()

	first := broker.CreateExchange("corp")
	second := broker.CreateExchange("corp")

	if first != second {
		t.Fatal("CreateExchange() returned different exchanges for the same name")
	}
	if len(broker.Exchanges) != 1 {
		t.Fatalf("exchange count = %d, want 1", len(broker.Exchanges))
	}
}
