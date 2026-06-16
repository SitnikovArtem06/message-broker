-- +goose Up
CREATE TABLE exchanges (
    name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE queues (
    exchange_name TEXT NOT NULL,
    name TEXT NOT NULL,
    durable BOOLEAN NOT NULL,
    auto_delete BOOLEAN NOT NULL,
    max_attempts INTEGER NOT NULL DEFAULT 0 CHECK (max_attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (exchange_name, name),
    FOREIGN KEY (exchange_name) REFERENCES exchanges(name) ON DELETE CASCADE
);

CREATE TABLE queue_filters (
    exchange_name TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    filter TEXT NOT NULL,
    PRIMARY KEY (exchange_name, queue_name, filter),
    FOREIGN KEY (exchange_name, queue_name) REFERENCES queues(exchange_name, name) ON DELETE CASCADE
);

CREATE TABLE deliveries (
    id TEXT PRIMARY KEY,
    exchange_name TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ready', 'in_flight')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    consumer_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (exchange_name, queue_name) REFERENCES queues(exchange_name, name) ON DELETE CASCADE
);

CREATE INDEX idx_deliveries_queue_status_created
    ON deliveries(exchange_name, queue_name, status, created_at);

CREATE TABLE dead_letters (
    id TEXT PRIMARY KEY,
    exchange_name TEXT NOT NULL,
    source_queue TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (exchange_name)
        REFERENCES exchanges(name)
        ON DELETE CASCADE
);

CREATE INDEX idx_dead_letters_exchange_created
    ON dead_letters(exchange_name, created_at);

-- +goose Down
DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS queue_filters;
DROP TABLE IF EXISTS queues;
DROP TABLE IF EXISTS exchanges;
