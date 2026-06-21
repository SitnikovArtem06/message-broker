package broker_handler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/SitnikovArtem06/message-broker/internal/core"
	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
	"github.com/SitnikovArtem06/message-broker/internal/service"
	brokerpb "github.com/SitnikovArtem06/message-broker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BrokerHandler struct {
	brokerpb.UnimplementedBrokerServiceServer
	service *service.BrokerService
}

const TimeOutDisconnect = 5 * time.Millisecond

func NewBrokerHandler(service *service.BrokerService) *BrokerHandler {
	return &BrokerHandler{service: service}
}

func (h *BrokerHandler) CreateExchange(ctx context.Context, req *brokerpb.CreateExchangeRequest) (*brokerpb.CreateExchangeResponse, error) {
	exchange, err := h.service.CreateExchange(ctx, req.GetName())
	if err != nil {
		return nil, mapError(err)
	}

	resp := &brokerpb.CreateExchangeResponse{Name: exchange.Name}
	return resp, nil
}

func (h *BrokerHandler) DeleteExchange(ctx context.Context, req *brokerpb.DeleteExchangeRequest) (*brokerpb.DeleteExchangeResponse, error) {
	if err := h.service.DeleteExchange(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.DeleteExchangeResponse{}, nil

}

func (h *BrokerHandler) RegisterQueue(ctx context.Context, req *brokerpb.RegisterQueueRequest) (*brokerpb.RegisterQueueResponse, error) {
	var filters []core.RoutingFilter
	for _, f := range req.GetFilters() {
		filters = append(filters, core.RoutingFilter(f))
	}
	queue, err := h.service.RegisterQueue(
		ctx,
		req.GetExchangeName(),
		req.GetQueueName(),
		req.GetDurable(),
		req.GetAutoDelete(),
		int(req.GetMaxAttempts()),
		filters,
	)
	if err != nil {
		return nil, mapError(err)
	}

	resp := &brokerpb.RegisterQueueResponse{ExchangeName: req.GetExchangeName(), QueueName: queue.Name}
	return resp, nil
}

func (h *BrokerHandler) DeleteQueue(ctx context.Context, req *brokerpb.DeleteQueueRequest) (*brokerpb.DeleteQueueResponse, error) {
	if err := h.service.DeleteQueue(ctx, req.GetExchangeName(), req.GetQueueName()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.DeleteQueueResponse{}, nil
}

func (h *BrokerHandler) Publish(ctx context.Context, req *brokerpb.PublishRequest) (*brokerpb.PublishResponse, error) {
	if err := h.service.Publish(ctx, req.GetExchangeName(), req.GetRoutingKey(), req.GetPayload()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.PublishResponse{}, nil
}

func (h *BrokerHandler) Fetch(ctx context.Context, req *brokerpb.FetchRequest) (*brokerpb.FetchResponse, error) {
	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	delivery, err := h.service.BlockingFetch(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetConsumerId(), timeout)
	if err != nil {
		return nil, mapError(err)
	}

	resp := &brokerpb.FetchResponse{
		Delivery: &brokerpb.DeliveryMessage{
			DeliveryId: delivery.ID,
			RoutingKey: string(delivery.Message.RoutingKey),
			Payload:    delivery.Message.Payload,
			Attempts:   int32(delivery.Attempts)}}
	return resp, nil
}

func (h *BrokerHandler) StreamFetch(req *brokerpb.SFetchRequest, stream grpc.ServerStreamingServer[brokerpb.SFetchResponse]) error {
	ctx := stream.Context()

	if err := h.service.AddConsumer(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetConsumerId()); err != nil {
		return mapError(err)
	}

	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), TimeOutDisconnect)
		defer cancel()

		_ = h.service.DisconnectConsumer(
			disconnectCtx,
			req.GetExchangeName(),
			req.GetQueueName(),
			req.GetConsumerId(),
		)
	}()

	for {
		delivery, err := h.service.StreamFetch(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetConsumerId())
		if err != nil {
			slog.Error("critical delivery error", "operation", "stream_fetch", "exchange", req.GetExchangeName(), "queue", req.GetQueueName(), "consumer", req.GetConsumerId(), "err", err)
			return mapError(err)
		}

		resp := &brokerpb.SFetchResponse{
			Delivery: &brokerpb.DeliveryMessage{
				DeliveryId: delivery.ID,
				RoutingKey: string(delivery.Message.RoutingKey),
				Payload:    delivery.Message.Payload,
				Attempts:   int32(delivery.Attempts),
			},
		}

		if err := stream.Send(resp); err != nil {
			slog.Error("critical delivery error", "operation", "stream_send", "exchange", req.GetExchangeName(), "queue", req.GetQueueName(), "consumer", req.GetConsumerId(), "delivery_id", delivery.ID, "err", err)
			return err
		}
	}
}

func (h *BrokerHandler) Ack(ctx context.Context, req *brokerpb.AckRequest) (*brokerpb.AckResponse, error) {
	if err := h.service.Ack(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetDeliveryId(), req.GetConsumerId()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.AckResponse{}, nil
}

func (h *BrokerHandler) Nack(ctx context.Context, req *brokerpb.NackRequest) (*brokerpb.NackResponse, error) {
	if err := h.service.NAck(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetDeliveryId(), req.GetConsumerId()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.NackResponse{}, nil
}

func (h *BrokerHandler) AddConsumer(ctx context.Context, req *brokerpb.AddConsumerRequest) (*brokerpb.AddConsumerResponse, error) {
	if err := h.service.AddConsumer(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetConsumerId()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.AddConsumerResponse{}, nil
}

func (h *BrokerHandler) DisconnectConsumer(ctx context.Context, req *brokerpb.DisconnectConsumerRequest) (*brokerpb.DisconnectConsumerResponse, error) {
	if err := h.service.DisconnectConsumer(ctx, req.GetExchangeName(), req.GetQueueName(), req.GetConsumerId()); err != nil {
		return nil, mapError(err)
	}

	return &brokerpb.DisconnectConsumerResponse{}, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, errs.InvalidRoutingKey),
		errors.Is(err, errs.FiltersIncorrect),
		errors.Is(err, errs.MaxAttemptsIncorrect),
		errors.Is(err, errs.MessageTooLarge),
		errors.Is(err, errs.RoutingKeyTooLong),
		errors.Is(err, errs.TooManyQueueFilters),
		errors.Is(err, errs.TooManyInFlight),
		errors.Is(err, errs.QueueFlagsConflict),
		errors.Is(err, errs.MessageNotMatch):
		if errors.Is(err, errs.TooManyInFlight) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, errs.ExchangeNotFound),
		errors.Is(err, errs.QueueNotFound),
		errors.Is(err, errs.DeliveryNotFound),
		errors.Is(err, errs.ConsumerNotFound),
		errors.Is(err, errs.QueueEmpty):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errs.QueueHasConsumers),
		errors.Is(err, errs.ExchangeHasConsumers):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, errs.ExchangeAlreadyExist),
		errors.Is(err, errs.QueueAlreadyExist):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, errs.DeliveryOwnerMismatch):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, errs.Unauthorized):
		return status.Error(codes.Unauthenticated, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
