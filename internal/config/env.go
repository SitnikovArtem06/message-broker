package config

import (
	"fmt"
	"os"
	"strconv"
)

func getEnv(key string, fallback string) string {
	value, ok := lookupEnv(key)
	if !ok || value == "" {
		return fallback
	}

	return value
}

func lookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}

func getPositiveIntEnv(key string, fallback int) (int, error) {
	value, ok := lookupEnv(key)
	if !ok || value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}

	return parsed, nil
}
