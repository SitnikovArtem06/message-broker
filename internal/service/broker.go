package service

import (
	"context"

	"github.com/SitnikovArtem06/message-broker/internal/core"
	"github.com/SitnikovArtem06/message-broker/internal/storage"
)

type Repository interface {
	SaveExchange(ctx context.Context, exchangeName string) error
	DeleteExchange(ctx context.Context, exchangeName string) error
	SaveQueue(ctx context.Context, exchangeName string, queue *core.Queue) error
	DeleteQueue(ctx context.Context, exchangeName string, queueName string) error
	SaveReadyDelivery(ctx context.Context, exchangeName string, queueName string, delivery core.Delivery) error
	MarkInFlight(ctx context.Context, deliveryID string, consumerID string, attempts int) error
	MarkReady(ctx context.Context, deliveryID string) error
	DeleteDelivery(ctx context.Context, deliveryID string) error
	SaveDeadLetter(ctx context.Context, exchangeName string, letter core.DeadLetter) error
	LoadState(ctx context.Context) (storage.BrokerState, error)
}

type BrokerService struct {
	broker *core.Broker
	repo   Repository
}

func NewBrokerService(broker *core.Broker, repo Repository) *BrokerService {
	return &BrokerService{
		broker: broker,
		repo:   repo,
	}
}

func (s *BrokerService) CreateExchange(ctx context.Context, name string) (*core.Exchange, error) {
	exchange := s.broker.CreateExchange(name)
	if err := s.repo.SaveExchange(ctx, name); err != nil {
		return nil, err
	}

	return exchange, nil
}

func (s *BrokerService) DeleteExchange(ctx context.Context, name string) error {
	if err := s.broker.DeleteExchange(name); err != nil {
		return err
	}

	return s.repo.DeleteExchange(ctx, name)
}

func (s *BrokerService) RegisterQueue(
	ctx context.Context,
	exchangeName string,
	queueName string,
	isDurable bool,
	isAutoDelete bool,
	filters []core.RoutingFilter,
) (*core.Queue, error) {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return nil, err
	}

	queue, err := exchange.RegisterQueue(queueName, isDurable, isAutoDelete, filters)
	if err != nil {
		return nil, err
	}

	if queue.IsDurable {
		if err := s.repo.SaveQueue(ctx, exchangeName, queue); err != nil {
			return nil, err
		}
	}

	return queue, nil
}

func (s *BrokerService) DeleteQueue(ctx context.Context, exchangeName string, queueName string) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return err
	}

	if err := exchange.DeleteQueue(queueName); err != nil {
		return err
	}

	if queue.IsDurable {
		return s.repo.DeleteQueue(ctx, exchangeName, queueName)
	}

	return nil
}

func (s *BrokerService) Publish(ctx context.Context, exchangeName string, routingKey string, payload []byte) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	published, err := exchange.Publish(routingKey, payload)
	if err != nil {
		return err
	}

	for _, item := range published {
		if !item.Queue.IsDurable {
			continue
		}

		if err := s.repo.SaveReadyDelivery(ctx, exchangeName, item.QueueName, item.Delivery); err != nil {
			return err
		}
	}

	return nil
}

func (s *BrokerService) Fetch(ctx context.Context, exchangeName string, queueName string, consumerID string) (core.Delivery, error) {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return core.Delivery{}, err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return core.Delivery{}, err
	}

	delivery, err := exchange.Fetch(queueName, consumerID)
	if err != nil {
		return core.Delivery{}, err
	}

	if queue.IsDurable {
		if err := s.repo.MarkInFlight(ctx, delivery.ID, consumerID, delivery.Attempts); err != nil {
			return core.Delivery{}, err
		}
	}

	return delivery, nil
}

func (s *BrokerService) Ack(ctx context.Context, exchangeName string, queueName string, deliveryID string, consumerID string) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return err
	}

	if err := exchange.Ack(queueName, deliveryID, consumerID); err != nil {
		return err
	}

	if queue.IsDurable {
		return s.repo.DeleteDelivery(ctx, deliveryID)
	}

	return nil
}

func (s *BrokerService) NAck(ctx context.Context, exchangeName string, queueName string, deliveryID string, consumerID string) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return err
	}

	result, err := exchange.NAck(queueName, deliveryID, consumerID)
	if err != nil {
		return err
	}

	if result.IsDead {
		if queue.IsDurable {
			if err := s.repo.DeleteDelivery(ctx, result.Delivery.ID); err != nil {
				return err
			}
		}

		return s.repo.SaveDeadLetter(ctx, exchangeName, result.DeadLetter)
	}

	if queue.IsDurable {
		return s.repo.MarkReady(ctx, result.Delivery.ID)
	}

	return nil
}

func (s *BrokerService) AddConsumer(_ context.Context, exchangeName string, queueName string, consumerID string) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	return exchange.AddConsumer(queueName, consumerID)
}

func (s *BrokerService) DisconnectConsumer(ctx context.Context, exchangeName string, queueName string, consumerID string) error {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return err
	}

	returned, err := exchange.DisconnectConsumer(queueName, consumerID)
	if err != nil {
		return err
	}

	if !queue.IsDurable {
		return nil
	}

	for _, delivery := range returned {
		if err := s.repo.MarkReady(ctx, delivery.ID); err != nil {
			return err
		}
	}

	return nil
}

func (s *BrokerService) Restore(ctx context.Context) error {
	state, err := s.repo.LoadState(ctx)
	if err != nil {
		return err
	}

	for _, exchangeName := range state.Exchanges {
		s.broker.CreateExchange(exchangeName)
	}

	for _, queueState := range state.Queues {
		exchange, err := s.broker.GetExchange(queueState.ExchangeName)
		if err != nil {
			return err
		}
		queue, err := exchange.RegisterQueue(
			queueState.Name,
			queueState.IsDurable,
			queueState.IsAutoDelete,
			queueState.Filters,
		)
		if err != nil {
			return err
		}
		queue.MaxAttempts = queueState.MaxAttempts
	}

	for _, deliveryState := range state.Deliveries {
		exchange, err := s.broker.GetExchange(deliveryState.ExchangeName)
		if err != nil {
			return err
		}

		queue, err := exchange.GetQueue(deliveryState.QueueName)
		if err != nil {
			return err
		}

		queue.RestoreReady(deliveryState.Delivery)
		if err := s.repo.MarkReady(ctx, deliveryState.Delivery.ID); err != nil {
			return err
		}
	}

	return nil
}
