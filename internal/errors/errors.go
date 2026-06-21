package errors

import "errors"

var (
	ExchangeAlreadyExist  = errors.New("exchange with this name already exists")
	ExchangeNotFound      = errors.New("exchange not found")
	ExchangeHasConsumers  = errors.New("exchange has queues with active consumers")
	QueueAlreadyExist     = errors.New("queue with this name already exists")
	QueueNotFound         = errors.New("queue not found")
	QueueHasConsumers     = errors.New("queue has active consumers")
	QueueFlagsConflict    = errors.New("IsDurable and IsAutoDelete cant be true in one time")
	QueueEmpty            = errors.New("queue empty")
	FiltersIncorrect      = errors.New("filters are Incorrect")
	MaxAttemptsIncorrect  = errors.New("max attempts must be non-negative")
	MessageTooLarge       = errors.New("message exceeds maximum size")
	RoutingKeyTooLong     = errors.New("routing key exceeds maximum length")
	TooManyQueueFilters   = errors.New("too many queue filters")
	MessageNotMatch       = errors.New("message doesnt match to queue filters")
	InvalidRoutingKey     = errors.New("invalid routing key")
	DeliveryNotFound      = errors.New("delivery not found")
	ConsumerNotFound      = errors.New("consumer not found")
	TooManyInFlight       = errors.New("consumer reached max in-flight deliveries")
	DeliveryOwnerMismatch = errors.New("delivery belongs to another consumer")
	Unauthorized          = errors.New("invalid root token")
)
