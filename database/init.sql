CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    status TEXT NOT NULL,
    raw_event JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS maintenance_events (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    event_category TEXT NOT NULL,
    description TEXT NOT NULL,
    technician TEXT,
    severity TEXT NOT NULL,
    raw_event JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS operator_events (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    notes TEXT,
    raw_event JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    source_event_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    alert_message TEXT NOT NULL,
    severity TEXT NOT NULL,
    threshold_value DOUBLE PRECISION,
    observed_value DOUBLE PRECISION,
    raw_event JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS actuator_commands (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    actuator_id TEXT NOT NULL,
    actuator_type TEXT NOT NULL,
    command TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_event JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
