package config

import (
	"fmt"
	"time"
)

type DBConfig struct {
	Host           string
	Port           string
	User           string
	Password       string
	Name           string
	SSLMode        string
	ConnectRetries int
	RetryDelay     time.Duration
}

func loadDBConfig() (DBConfig, error) {
	connectRetries, err := getPositiveIntEnv("DB_CONNECT_RETRIES", 5)
	if err != nil {
		return DBConfig{}, err
	}
	retryDelayMs, err := getPositiveIntEnv("DB_CONNECT_RETRY_DELAY_MS", 1000)
	if err != nil {
		return DBConfig{}, err
	}

	cfg := DBConfig{
		Host:           getEnv("DB_HOST", "localhost"),
		Port:           getEnv("DB_PORT", "5432"),
		User:           getEnv("DB_USER", "message_broker"),
		Password:       getEnv("DB_PASSWORD", "message_broker"),
		Name:           getEnv("DB_NAME", "message_broker"),
		SSLMode:        getEnv("DB_SSLMODE", "disable"),
		ConnectRetries: connectRetries,
		RetryDelay:     time.Duration(retryDelayMs) * time.Millisecond,
	}

	return cfg, nil
}

func (c DBConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.Name,
		c.SSLMode,
	)
}
