package brokerclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	brokerpb "github.com/SitnikovArtem06/message-broker/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	conn       *grpc.ClientConn
	api        brokerpb.BrokerServiceClient
	rootToken  string
	consumerID string

	mu                 sync.Mutex
	fetchSubscriptions map[subscriptionKey]struct{}
}

type subscriptionKey struct {
	exchangeName string
	queueName    string
}

func New(addr string, opts ...Option) (*Client, error) {
	cfg := clientOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.rootToken == "" {
		return nil, errors.New("root token is required")
	}
	if cfg.consumerID == "" {
		cfg.consumerID = uuid.NewString()
	}

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	dialOptions = append(dialOptions, cfg.dialOptions...)

	conn, err := grpc.NewClient(addr, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}

	return &Client{
		conn:               conn,
		api:                brokerpb.NewBrokerServiceClient(conn),
		rootToken:          cfg.rootToken,
		consumerID:         cfg.consumerID,
		fetchSubscriptions: make(map[subscriptionKey]struct{}),
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	subscriptions := make([]subscriptionKey, 0, len(c.fetchSubscriptions))
	for key := range c.fetchSubscriptions {
		subscriptions = append(subscriptions, key)
	}
	c.mu.Unlock()

	for _, key := range subscriptions {
		_ = c.DisconnectConsumer(context.Background(), key.exchangeName, key.queueName)
	}

	return c.conn.Close()
}

func (c *Client) CreateExchange(ctx context.Context, name string) error {
	_, err := c.api.CreateExchange(c.withAuth(ctx), &brokerpb.CreateExchangeRequest{Name: name})
	return err
}

func (c *Client) DeleteExchange(ctx context.Context, name string) error {
	_, err := c.api.DeleteExchange(c.withAuth(ctx), &brokerpb.DeleteExchangeRequest{Name: name})
	return err
}

func (c *Client) RegisterQueue(
	ctx context.Context,
	exchangeName string,
	queueName string,
	durable bool,
	autoDelete bool,
	opts ...QueueOption,
) error {
	cfg := queueOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}

	_, err := c.api.RegisterQueue(c.withAuth(ctx), &brokerpb.RegisterQueueRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		Durable:      durable,
		AutoDelete:   autoDelete,
		Filters:      cfg.filters,
		MaxAttempts:  cfg.maxAttempts,
	})
	return err
}

func (c *Client) DeleteQueue(ctx context.Context, exchangeName string, queueName string) error {
	_, err := c.api.DeleteQueue(c.withAuth(ctx), &brokerpb.DeleteQueueRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
	})
	return err
}

func (c *Client) Publish(ctx context.Context, exchangeName string, routingKey string, payload []byte) error {
	_, err := c.api.Publish(c.withAuth(ctx), &brokerpb.PublishRequest{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Payload:      payload,
	})
	return err
}

func (c *Client) Fetch(
	ctx context.Context,
	exchangeName string,
	queueName string,
	timeout time.Duration,
) (*Message, error) {
	if err := c.ensureFetchSubscription(ctx, exchangeName, queueName); err != nil {
		return nil, err
	}

	resp, err := c.api.Fetch(c.withAuth(ctx), &brokerpb.FetchRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		ConsumerId:   c.consumerID,
		TimeoutMs:    timeout.Milliseconds(),
	})
	if err != nil {
		return nil, err
	}

	return c.newMessage(exchangeName, queueName, resp.GetDelivery()), nil
}

func (c *Client) StreamFetch(ctx context.Context, exchangeName string, queueName string) (*MessageStream, error) {
	stream, err := c.api.StreamFetch(c.withAuth(ctx), &brokerpb.SFetchRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		ConsumerId:   c.consumerID,
	})
	if err != nil {
		return nil, err
	}

	return &MessageStream{
		client:       c,
		exchangeName: exchangeName,
		queueName:    queueName,
		stream:       stream,
	}, nil
}

func (c *Client) AddConsumer(ctx context.Context, exchangeName string, queueName string) error {
	_, err := c.api.AddConsumer(c.withAuth(ctx), &brokerpb.AddConsumerRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		ConsumerId:   c.consumerID,
	})
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.fetchSubscriptions[subscriptionKey{exchangeName: exchangeName, queueName: queueName}] = struct{}{}
	c.mu.Unlock()

	return nil
}

func (c *Client) DisconnectConsumer(ctx context.Context, exchangeName string, queueName string) error {
	_, err := c.api.DisconnectConsumer(c.withAuth(ctx), &brokerpb.DisconnectConsumerRequest{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		ConsumerId:   c.consumerID,
	})
	if err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.fetchSubscriptions, subscriptionKey{exchangeName: exchangeName, queueName: queueName})
	c.mu.Unlock()

	return nil
}

func (c *Client) ConsumerID() string {
	return c.consumerID
}

func (c *Client) ensureFetchSubscription(ctx context.Context, exchangeName string, queueName string) error {
	key := subscriptionKey{exchangeName: exchangeName, queueName: queueName}

	c.mu.Lock()
	_, ok := c.fetchSubscriptions[key]
	c.mu.Unlock()
	if ok {
		return nil
	}

	return c.AddConsumer(ctx, exchangeName, queueName)
}

func (c *Client) withAuth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-root-token", c.rootToken)
}

func (c *Client) newMessage(exchangeName string, queueName string, delivery *brokerpb.DeliveryMessage) *Message {
	if delivery == nil {
		return nil
	}

	return &Message{
		client:       c,
		exchangeName: exchangeName,
		queueName:    queueName,
		deliveryID:   delivery.GetDeliveryId(),
		routingKey:   delivery.GetRoutingKey(),
		payload:      delivery.GetPayload(),
		attempts:     delivery.GetAttempts(),
	}
}
