package brokerclient

import (
	"context"

	brokerpb "github.com/SitnikovArtem06/message-broker/proto"
)

type Message struct {
	client       *Client
	exchangeName string
	queueName    string
	deliveryID   string
	routingKey   string
	payload      []byte
	attempts     int32
}

func (m *Message) DeliveryID() string {
	return m.deliveryID
}

func (m *Message) RoutingKey() string {
	return m.routingKey
}

func (m *Message) Payload() []byte {
	return m.payload
}

func (m *Message) Attempts() int32 {
	return m.attempts
}

func (m *Message) Ack(ctx context.Context) error {
	_, err := m.client.api.Ack(m.client.withAuth(ctx), &brokerpb.AckRequest{
		ExchangeName: m.exchangeName,
		QueueName:    m.queueName,
		DeliveryId:   m.deliveryID,
		ConsumerId:   m.client.consumerID,
	})
	return err
}

func (m *Message) NAck(ctx context.Context) error {
	_, err := m.client.api.Nack(m.client.withAuth(ctx), &brokerpb.NackRequest{
		ExchangeName: m.exchangeName,
		QueueName:    m.queueName,
		DeliveryId:   m.deliveryID,
		ConsumerId:   m.client.consumerID,
	})
	return err
}
