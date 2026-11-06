CREATE TABLE IF NOT EXISTS valid_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, event_time),
    UNIQUE (kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS invalid_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT,
    amount_text TEXT,
    event_timestamp TEXT,
    source TEXT,
    error_reason TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS transaction_windows (
    window_start TIMESTAMPTZ PRIMARY KEY,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(18,2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    name TEXT PRIMARY KEY,
    max_event_time TIMESTAMPTZ
);

INSERT INTO pipeline_state (name, max_event_time)
VALUES ('transactions', NULL) ON CONFLICT (name) DO NOTHING;
