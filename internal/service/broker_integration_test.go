package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SitnikovArtem06/message-broker/internal/config"
	"github.com/SitnikovArtem06/message-broker/internal/core"
	"github.com/SitnikovArtem06/message-broker/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegrationRestoreReadyDurableDelivery(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t)
	repo := repository.NewRepository(pool)

	exchangeName := "it_restore_ready_" + uuid.NewString()
	queueName := "users"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = repo.DeleteExchange(cleanupCtx, exchangeName)
	})

	serviceBeforeRestart := newIntegrationBrokerService(repo)
	if _, err := serviceBeforeRestart.CreateExchange(ctx, exchangeName); err != nil {
		t.Fatalf("CreateExchange() error = %v", err)
	}
	if _, err := serviceBeforeRestart.RegisterQueue(ctx, exchangeName, queueName, true, false, 0, []core.RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := serviceBeforeRestart.Publish(ctx, exchangeName, "corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	serviceAfterRestart := newIntegrationBrokerService(repo)
	if err := serviceAfterRestart.Restore(ctx); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if err := serviceAfterRestart.AddConsumer(ctx, exchangeName, queueName, "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	delivery, err := serviceAfterRestart.BlockingFetch(ctx, exchangeName, queueName, "consumer-1", time.Second)
	if err != nil {
		t.Fatalf("BlockingFetch() error = %v", err)
	}
	if string(delivery.Message.Payload) != "payload" {
		t.Fatalf("payload = %q, want %q", delivery.Message.Payload, "payload")
	}
	if delivery.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", delivery.Attempts)
	}

	if err := serviceAfterRestart.Ack(ctx, exchangeName, queueName, delivery.ID, "consumer-1"); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestIntegrationRestoreInFlightDurableDeliveryAsReady(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t)
	repo := repository.NewRepository(pool)

	exchangeName := "it_restore_in_flight_" + uuid.NewString()
	queueName := "users"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = repo.DeleteExchange(cleanupCtx, exchangeName)
	})

	serviceBeforeRestart := newIntegrationBrokerService(repo)
	if _, err := serviceBeforeRestart.CreateExchange(ctx, exchangeName); err != nil {
		t.Fatalf("CreateExchange() error = %v", err)
	}
	if _, err := serviceBeforeRestart.RegisterQueue(ctx, exchangeName, queueName, true, false, 0, []core.RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := serviceBeforeRestart.AddConsumer(ctx, exchangeName, queueName, "consumer-1"); err != nil {
		t.Fatalf("AddConsumer(consumer-1) error = %v", err)
	}
	if err := serviceBeforeRestart.Publish(ctx, exchangeName, "corp.users.create", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := serviceBeforeRestart.BlockingFetch(ctx, exchangeName, queueName, "consumer-1", time.Second)
	if err != nil {
		t.Fatalf("first BlockingFetch() error = %v", err)
	}

	serviceAfterRestart := newIntegrationBrokerService(repo)
	if err := serviceAfterRestart.Restore(ctx); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if err := serviceAfterRestart.AddConsumer(ctx, exchangeName, queueName, "consumer-2"); err != nil {
		t.Fatalf("AddConsumer(consumer-2) error = %v", err)
	}

	redelivery, err := serviceAfterRestart.BlockingFetch(ctx, exchangeName, queueName, "consumer-2", time.Second)
	if err != nil {
		t.Fatalf("second BlockingFetch() error = %v", err)
	}
	if redelivery.ID != firstDelivery.ID {
		t.Fatalf("redelivery id = %q, want %q", redelivery.ID, firstDelivery.ID)
	}
	if redelivery.ConsumerID != "consumer-2" {
		t.Fatalf("consumer id = %q, want %q", redelivery.ConsumerID, "consumer-2")
	}
	if redelivery.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", redelivery.Attempts)
	}

	if err := serviceAfterRestart.Ack(ctx, exchangeName, queueName, redelivery.ID, "consumer-2"); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestIntegrationRestoreQueueMaxAttempts(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t)
	repo := repository.NewRepository(pool)

	exchangeName := "it_restore_max_attempts_" + uuid.NewString()
	queueName := "users"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = repo.DeleteExchange(cleanupCtx, exchangeName)
	})

	serviceBeforeRestart := newIntegrationBrokerService(repo)
	if _, err := serviceBeforeRestart.CreateExchange(ctx, exchangeName); err != nil {
		t.Fatalf("CreateExchange() error = %v", err)
	}
	if _, err := serviceBeforeRestart.RegisterQueue(ctx, exchangeName, queueName, true, false, 3, []core.RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}

	serviceAfterRestart := newIntegrationBrokerService(repo)
	if err := serviceAfterRestart.Restore(ctx); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	exchange, err := serviceAfterRestart.broker.GetExchange(exchangeName)
	if err != nil {
		t.Fatalf("GetExchange() error = %v", err)
	}
	queue, err := exchange.GetQueue(queueName)
	if err != nil {
		t.Fatalf("GetQueue() error = %v", err)
	}
	if queue.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", queue.MaxAttempts)
	}
}

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "message_broker_test",
				"POSTGRES_USER":     "message_broker",
				"POSTGRES_PASSWORD": "message_broker",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("Docker testcontainer is not available: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("failed to terminate PostgreSQL testcontainer: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host() error = %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort() error = %v", err)
	}

	dsn := fmt.Sprintf(
		"postgres://message_broker:message_broker@%s:%s/message_broker_test?sslmode=disable",
		host,
		port.Port(),
	)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("database ping error = %v", err)
	}
	if err := applyIntegrationMigrations(ctx, pool); err != nil {
		t.Fatalf("apply integration migrations: %v", err)
	}

	return pool
}

func applyIntegrationMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrationPath := filepath.Join("..", "..", "migrations", "00001_init.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		return err
	}

	upSQL, _, _ := strings.Cut(string(migration), "-- +goose Down")
	upSQL = strings.Replace(upSQL, "-- +goose Up", "", 1)

	_, err = pool.Exec(ctx, upSQL)
	return err
}

func newIntegrationBrokerService(repo Repository) *BrokerService {
	return NewBrokerService(core.NewBroker(), repo, config.LimitsConfig{
		MaxMessageSize:      1024 * 1024,
		MaxRoutingKeyLength: 255,
		MaxQueueFilters:     32,
		MaxInFlight:         32,
	})
}
