package repository

import (
	"context"
	stdsql "database/sql"

	"github.com/SitnikovArtem06/message-broker/internal/core"
	"github.com/SitnikovArtem06/message-broker/internal/storage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) SaveExchange(ctx context.Context, exchangeName string) error {
	sql := `INSERT INTO exchanges(name) VALUES($1) ON CONFLICT (name) DO NOTHING`

	if _, err := r.db.Exec(ctx, sql, exchangeName); err != nil {
		return err
	}

	return nil
}

func (r *Repository) DeleteExchange(ctx context.Context, exchangeName string) error {
	sql := `DELETE FROM exchanges WHERE name = $1`

	if _, err := r.db.Exec(ctx, sql, exchangeName); err != nil {
		return err
	}

	return nil
}

func (r *Repository) SaveQueue(ctx context.Context, exchangeName string, queue *core.Queue) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	queueSQL := `
		INSERT INTO queues (
			exchange_name,
			name,
			durable,
			auto_delete,
			max_attempts
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (exchange_name, name) DO NOTHING
	`

	if _, err := tx.Exec(
		ctx,
		queueSQL,
		exchangeName,
		queue.Name,
		queue.IsDurable,
		queue.IsAutoDelete,
		queue.MaxAttempts,
	); err != nil {
		return err
	}

	const filterSQL = `
		INSERT INTO queue_filters (
			exchange_name,
			queue_name,
			filter
		)
		VALUES ($1, $2, $3)
		ON CONFLICT (exchange_name, queue_name, filter) DO NOTHING
	`
	for _, filter := range queue.Filters {
		if _, err := tx.Exec(ctx, filterSQL, exchangeName, queue.Name, string(filter)); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *Repository) DeleteQueue(ctx context.Context, exchangeName string, queueName string) error {
	const sql = `DELETE FROM queues WHERE exchange_name = $1 AND name = $2`

	if _, err := r.db.Exec(ctx, sql, exchangeName, queueName); err != nil {
		return err
	}

	return nil
}

func (r *Repository) SaveReadyDelivery(ctx context.Context, exchangeName string, queueName string, delivery core.Delivery) error {
	const sql = `
		INSERT INTO deliveries (
			id,
			exchange_name,
			queue_name,
			routing_key,
			payload,
			status,
			attempts,
			consumer_id
		)
		VALUES ($1, $2, $3, $4, $5, 'ready', $6, NULL)
	`

	if _, err := r.db.Exec(
		ctx,
		sql,
		delivery.ID,
		exchangeName,
		queueName,
		string(delivery.Message.RoutingKey),
		delivery.Message.Payload,
		delivery.Attempts,
	); err != nil {
		return err
	}

	return nil
}

func (r *Repository) MarkInFlight(ctx context.Context, deliveryID string, consumerID string, attempts int) error {
	const sql = `
		UPDATE deliveries
		SET status = 'in_flight',
			consumer_id = $2,
			attempts = $3,
			updated_at = now()
		WHERE id = $1
	`

	if _, err := r.db.Exec(ctx, sql, deliveryID, consumerID, attempts); err != nil {
		return err
	}

	return nil
}

func (r *Repository) MarkReady(ctx context.Context, deliveryID string) error {
	const sql = `
		UPDATE deliveries
		SET status = 'ready',
			consumer_id = NULL,
			updated_at = now()
		WHERE id = $1
	`

	if _, err := r.db.Exec(ctx, sql, deliveryID); err != nil {
		return err
	}

	return nil
}

func (r *Repository) DeleteDelivery(ctx context.Context, deliveryID string) error {
	const sql = `DELETE FROM deliveries WHERE id = $1`

	if _, err := r.db.Exec(ctx, sql, deliveryID); err != nil {
		return err
	}

	return nil
}

func (r *Repository) SaveDeadLetter(ctx context.Context, exchangeName string, letter core.DeadLetter) error {
	const sql = `
		INSERT INTO dead_letters (
			id,
			exchange_name,
			source_queue,
			routing_key,
			payload,
			reason,
			attempts
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	if _, err := r.db.Exec(
		ctx,
		sql,
		uuid.New().String(),
		exchangeName,
		letter.SourceQueue,
		string(letter.Message.RoutingKey),
		letter.Message.Payload,
		letter.Reason,
		letter.Attempts,
	); err != nil {
		return err
	}

	return nil
}

func (r *Repository) LoadState(ctx context.Context) (storage.BrokerState, error) {
	var state storage.BrokerState

	exchanges, err := r.loadExchanges(ctx)
	if err != nil {
		return storage.BrokerState{}, err
	}
	state.Exchanges = exchanges

	queues, err := r.loadQueues(ctx)
	if err != nil {
		return storage.BrokerState{}, err
	}
	state.Queues = queues

	deliveries, err := r.loadDeliveries(ctx)
	if err != nil {
		return storage.BrokerState{}, err
	}
	state.Deliveries = deliveries

	return state, nil
}

func (r *Repository) loadExchanges(ctx context.Context) ([]string, error) {
	const sql = `SELECT name FROM exchanges ORDER BY created_at, name`

	rows, err := r.db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var exchanges []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		exchanges = append(exchanges, name)
	}

	return exchanges, rows.Err()
}

type queueKey struct {
	exchangeName string
	queueName    string
}

func (r *Repository) loadQueues(ctx context.Context) ([]storage.QueueState, error) {
	const sql = `
		SELECT
			q.exchange_name,
			q.name,
			q.durable,
			q.auto_delete,
			q.max_attempts,
			qf.filter
		FROM queues q
		LEFT JOIN queue_filters qf
			ON q.exchange_name = qf.exchange_name
			AND q.name = qf.queue_name
		ORDER BY q.exchange_name, q.name, qf.filter
	`

	rows, err := r.db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queuesByKey := make(map[queueKey]*storage.QueueState)
	var order []queueKey

	for rows.Next() {
		var (
			queue  storage.QueueState
			filter stdsql.NullString
			key    queueKey
		)
		if err := rows.Scan(
			&queue.ExchangeName,
			&queue.Name,
			&queue.IsDurable,
			&queue.IsAutoDelete,
			&queue.MaxAttempts,
			&filter,
		); err != nil {
			return nil, err
		}

		key = queueKey{exchangeName: queue.ExchangeName, queueName: queue.Name}
		storedQueue, ok := queuesByKey[key]
		if !ok {
			queuesByKey[key] = &queue
			storedQueue = &queue
			order = append(order, key)
		}
		if filter.Valid {
			storedQueue.Filters = append(storedQueue.Filters, core.RoutingFilter(filter.String))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	queues := make([]storage.QueueState, 0, len(order))
	for _, key := range order {
		queues = append(queues, *queuesByKey[key])
	}

	return queues, nil
}

func (r *Repository) loadDeliveries(ctx context.Context) ([]storage.DeliveryState, error) {
	const sql = `
		SELECT
			exchange_name,
			queue_name,
			id,
			routing_key,
			payload,
			attempts
		FROM deliveries
		ORDER BY created_at, id
	`

	rows, err := r.db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []storage.DeliveryState
	for rows.Next() {
		var (
			delivery   storage.DeliveryState
			routingKey string
		)
		if err := rows.Scan(
			&delivery.ExchangeName,
			&delivery.QueueName,
			&delivery.Delivery.ID,
			&routingKey,
			&delivery.Delivery.Message.Payload,
			&delivery.Delivery.Attempts,
		); err != nil {
			return nil, err
		}
		delivery.Delivery.Message.RoutingKey = core.RoutingKey(routingKey)
		deliveries = append(deliveries, delivery)
	}

	return deliveries, rows.Err()
}
