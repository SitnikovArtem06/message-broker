package brokerclient

import (
	"google.golang.org/grpc"
)

type Option func(*clientOptions)

type clientOptions struct {
	rootToken   string
	consumerID  string
	dialOptions []grpc.DialOption
}

func WithRootToken(token string) Option {
	return func(opts *clientOptions) {
		opts.rootToken = token
	}
}

func WithConsumerID(consumerID string) Option {
	return func(opts *clientOptions) {
		opts.consumerID = consumerID
	}
}

func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(cfg *clientOptions) {
		cfg.dialOptions = append(cfg.dialOptions, opts...)
	}
}

type QueueOption func(*queueOptions)

type queueOptions struct {
	maxAttempts int32
	filters     []string
}

func WithRoutingKey(filter string) QueueOption {
	return func(opts *queueOptions) {
		opts.filters = append(opts.filters, filter)
	}
}

func WithMaxAttempts(maxAttempts int) QueueOption {
	return func(opts *queueOptions) {
		opts.maxAttempts = int32(maxAttempts)
	}
}
