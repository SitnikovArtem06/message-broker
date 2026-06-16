package storage

import "github.com/SitnikovArtem06/message-broker/internal/core"

type BrokerState struct {
	Exchanges  []string
	Queues     []QueueState
	Deliveries []DeliveryState
}

type QueueState struct {
	ExchangeName string
	Name         string
	IsDurable    bool
	IsAutoDelete bool
	MaxAttempts  int
	Filters      []core.RoutingFilter
}

type DeliveryState struct {
	ExchangeName string
	QueueName    string
	Delivery     core.Delivery
}
