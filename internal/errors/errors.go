package errors

import "errors"

var (
	ExchangeAlreadyExist = errors.New("exchange with this name already exists")
	ExchangeNotFound     = errors.New("exchange not found")
	QueueAlreadyExist = errors.New("queue with this name already exists")
	QueueNotFound     = errors.New("queue not found")
	QueueFlagsConflict = errors.New("IsDurable and IsAutoDelete cant be true in one time")
	FiltersIncorrect = errors.New("Filters are Incorrect")
	QueueEmpty = errors.New("queue empty")
	MessageNotMatch = errors.New("message doesnt match to queue filters")
	InvalidRoutingKey = errors.New("invalid routing key")
)
