package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/SitnikovArtem06/message-broker/internal/config"
	"github.com/SitnikovArtem06/message-broker/internal/core"
	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
	"github.com/SitnikovArtem06/message-broker/internal/repository"
	"github.com/google/uuid"
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
	LoadState(ctx context.Context) (repository.BrokerState, error)
}

type BrokerService struct {
	broker *core.Broker
	repo   Repository
	limits config.LimitsConfig
}

const streamFetchTimeout = 30 * time.Second

func NewBrokerService(broker *core.Broker, repo Repository, cfg config.LimitsConfig) *BrokerService {
	return &BrokerService{broker: broker, repo: repo, limits: cfg}
}

func (s *BrokerService) CreateExchange(ctx context.Context, name string) (*core.Exchange, error) {
	exchange := s.broker.CreateExchange(name)
	if err := s.repo.SaveExchange(ctx, name); err != nil {
		return nil, err
	}
	slog.Info("exchange created", "exchange", name)

	return exchange, nil
}

func (s *BrokerService) DeleteExchange(ctx context.Context, name string) error {
	exchange, err := s.broker.GetExchange(name)
	if err != nil {
		return err
	}
	if exchange.HasActiveConsumers() {
		return errs.ExchangeHasConsumers
	}

	if err := s.broker.DeleteExchange(name); err != nil {
		return err
	}

	if err := s.repo.DeleteExchange(ctx, name); err != nil {
		return err
	}
	slog.Info("exchange deleted", "exchange", name)
	return nil
}

func (s *BrokerService) RegisterQueue(
	ctx context.Context,
	exchangeName string,
	queueName string,
	isDurable bool,
	isAutoDelete bool,
	maxAttempts int,
	filters []core.RoutingFilter,
) (*core.Queue, error) {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return nil, err
	}

	if len(filters) > s.limits.MaxQueueFilters {
		return nil, errs.TooManyQueueFilters
	}

	queue, err := exchange.RegisterQueue(queueName, isDurable, isAutoDelete, maxAttempts, filters)
	if err != nil {
		return nil, err
	}

	if queue.IsDurable {
		if err := s.repo.SaveQueue(ctx, exchangeName, queue); err != nil {
			return nil, err
		}
	}
	slog.Info("queue created", "exchange", exchangeName, "queue", queueName, "durable", isDurable, "auto_delete", isAutoDelete, "max_attempts", maxAttempts)

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
		if err := s.repo.DeleteQueue(ctx, exchangeName, queueName); err != nil {
			return err
		}
	}

	slog.Info("queue deleted", "exchange", exchangeName, "queue", queueName)
	return nil
}

func (s *BrokerService) Publish(
	ctx context.Context,
	exchangeName string,
	routingKey string,
	payload []byte,
) error {
	if len(payload) > s.limits.MaxMessageSize {
		return errs.MessageTooLarge
	}
	if len(routingKey) > s.limits.MaxRoutingKeyLength {
		return errs.RoutingKeyTooLong
	}

	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return err
	}

	published, err := exchange.Route(routingKey, payload)
	if err != nil {
		return err
	}

	for _, item := range published {
		if !item.Queue.IsDurable {
			if err := exchange.EnqueueDelivery(item.QueueName, core.Delivery{
				ID:      uuid.New().String(),
				Message: item.Message,
			}); err != nil {
				return err
			}
			continue
		}

		delivery := core.Delivery{
			ID:      uuid.New().String(),
			Message: item.Message,
		}

		if err := s.repo.SaveReadyDelivery(ctx, exchangeName, item.QueueName, delivery); err != nil {
			return err
		}

		if err := exchange.EnqueueDelivery(item.QueueName, delivery); err != nil {
			return err
		}
	}

	return nil
}

func (s *BrokerService) BlockingFetch(
	ctx context.Context,
	exchangeName string,
	queueName string,
	consumerID string,
	timeout time.Duration,
) (core.Delivery, error) {
	exchange, err := s.broker.GetExchange(exchangeName)
	if err != nil {
		return core.Delivery{}, err
	}

	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		return core.Delivery{}, err
	}

	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	for {
		delivery, waitCh, err := exchange.FetchOrWait(queueName, consumerID, s.limits.MaxInFlight)
		if err == nil {
			if queue.IsDurable {
				if err := s.repo.MarkInFlight(ctx, delivery.ID, consumerID, delivery.Attempts); err != nil {
					slog.Error("critical delivery error", "operation", "mark_in_flight", "exchange", exchangeName, "queue", queueName, "consumer", consumerID, "delivery_id", delivery.ID, "err", err)
					return core.Delivery{}, err
				}
			}
			return delivery, nil
		}

		if !errors.Is(err, errs.QueueEmpty) {
			return core.Delivery{}, err
		}

		if timeout <= 0 {
			return core.Delivery{}, errs.QueueEmpty
		}

		select {
		case <-ctx.Done():
			return core.Delivery{}, ctx.Err()
		case <-timer.C:
			return core.Delivery{}, errs.QueueEmpty
		case <-waitCh:
		}
	}
}

func (s *BrokerService) StreamFetch(
	ctx context.Context,
	exchangeName string,
	queueName string,
	consumerID string,
) (core.Delivery, error) {
	for {
		delivery, err := s.BlockingFetch(ctx, exchangeName, queueName, consumerID, streamFetchTimeout)
		if err == nil {
			return delivery, nil
		}
		if !errors.Is(err, errs.QueueEmpty) {
			return core.Delivery{}, err
		}
	}
}

func (s *BrokerService) Ack(
	ctx context.Context,
	exchangeName string,
	queueName string,
	deliveryID string,
	consumerID string,
) error {
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

func (s *BrokerService) NAck(
	ctx context.Context,
	exchangeName string,
	queueName string,
	deliveryID string,
	consumerID string,
) error {
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
				slog.Error("critical delivery error", "operation", "delete_delivery_before_dead_letter", "exchange", exchangeName, "queue", queueName, "consumer", consumerID, "delivery_id", result.Delivery.ID, "err", err)
				return err
			}
		}

		if err := s.repo.SaveDeadLetter(ctx, exchangeName, result.DeadLetter); err != nil {
			slog.Error("critical delivery error", "operation", "save_dead_letter", "exchange", exchangeName, "queue", queueName, "consumer", consumerID, "delivery_id", result.Delivery.ID, "err", err)
			return err
		}
		return nil
	}

	if queue.IsDurable {
		if err := s.repo.MarkReady(ctx, result.Delivery.ID); err != nil {
			slog.Error("critical delivery error", "operation", "mark_ready", "exchange", exchangeName, "queue", queueName, "consumer", consumerID, "delivery_id", result.Delivery.ID, "err", err)
			return err
		}
		return nil
	}

	return nil
}

func (s *BrokerService) AddConsumer(_ context.Context,
	exchangeName string,
	queueName string,
	consumerID string,
) error {
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
			slog.Error("critical delivery error", "operation", "mark_ready_on_disconnect", "exchange", exchangeName, "queue", queueName, "consumer", consumerID, "delivery_id", delivery.ID, "err", err)
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
		_, err = exchange.RegisterQueue(
			queueState.Name,
			queueState.IsDurable,
			queueState.IsAutoDelete,
			queueState.MaxAttempts,
			queueState.Filters,
		)
		if err != nil {
			return err
		}
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
