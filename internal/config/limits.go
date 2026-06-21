package config

const (
	defaultMaxMessageSize      = 1024 * 1024
	defaultMaxRoutingKeyLength = 255
	defaultMaxQueueFilters     = 32
	defaultMaxInFlight         = 32
)

type LimitsConfig struct {
	MaxMessageSize      int
	MaxRoutingKeyLength int
	MaxQueueFilters     int
	MaxInFlight         int
}

func loadLimitsConfig() (LimitsConfig, error) {
	cfg := LimitsConfig{
		MaxMessageSize:      defaultMaxMessageSize,
		MaxRoutingKeyLength: defaultMaxRoutingKeyLength,
		MaxQueueFilters:     defaultMaxQueueFilters,
		MaxInFlight:         defaultMaxInFlight,
	}

	maxMessageSize, err := getPositiveIntEnv("MAX_MESSAGE_SIZE", defaultMaxMessageSize)
	if err != nil {
		return LimitsConfig{}, err
	}
	maxRoutingKeyLength, err := getPositiveIntEnv("MAX_ROUTING_KEY_LENGTH", defaultMaxRoutingKeyLength)
	if err != nil {
		return LimitsConfig{}, err
	}
	maxQueueFilters, err := getPositiveIntEnv("MAX_QUEUE_FILTERS", defaultMaxQueueFilters)
	if err != nil {
		return LimitsConfig{}, err
	}
	maxInFlight, err := getPositiveIntEnv("MAX_IN_FLIGHT", defaultMaxInFlight)
	if err != nil {
		return LimitsConfig{}, err
	}

	cfg.MaxMessageSize = maxMessageSize
	cfg.MaxRoutingKeyLength = maxRoutingKeyLength
	cfg.MaxQueueFilters = maxQueueFilters
	cfg.MaxInFlight = maxInFlight

	return cfg, nil
}
