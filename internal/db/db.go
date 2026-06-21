package db

import (
	"context"
	"fmt"
	"time"

	"github.com/SitnikovArtem06/message-broker/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, cfg config.DBConfig) (*pgxpool.Pool, error) {
	var lastErr error

	for attempt := 1; attempt <= cfg.ConnectRetries; attempt++ {
		pool, err := pgxpool.New(ctx, cfg.DSN())
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}

		lastErr = err
		if attempt == cfg.ConnectRetries {
			break
		}

		timer := time.NewTimer(cfg.RetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("connect db after %d attempts: %w", cfg.ConnectRetries, lastErr)
}
