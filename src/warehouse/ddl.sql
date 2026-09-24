-- DDL for GitPulse Database Setup
-- Targets PostgreSQL (Redshift compatible structure)

-- 1. Raw Schema (Staging Land Zone)
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.github_events (
    id BIGINT PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    actor TEXT,          -- JSON string containing actor details
    repo TEXT,           -- JSON string containing repo details
    org TEXT,            -- JSON string containing org details (optional)
    payload TEXT,        -- JSON string containing event payload details
    public BOOLEAN,
    created_at VARCHAR(30) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on created_at to support fast date range scans
CREATE INDEX IF NOT EXISTS idx_github_events_created_at ON raw.github_events(created_at);

-- 2. Observability Schema
CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    dag_run_timestamp TIMESTAMP NOT NULL,
    rows_ingested BIGINT NOT NULL,
    freshness_lag_seconds INTEGER,
    dbt_tests_passed INTEGER DEFAULT 0,
    dbt_tests_failed INTEGER DEFAULT 0,
    duration_seconds INTEGER,
    status VARCHAR(20) NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Analytics Schema (for downstream consumption)
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.detected_anomalies (
    id SERIAL PRIMARY KEY,
    repo_id BIGINT NOT NULL,
    repo_name VARCHAR(255) NOT NULL,
    ds DATE NOT NULL,
    y DOUBLE PRECISION NOT NULL,            -- Actual event count
    yhat_lower DOUBLE PRECISION NOT NULL,   -- Forecast lower bound
    yhat_upper DOUBLE PRECISION NOT NULL,   -- Forecast upper bound
    anomaly_score DOUBLE PRECISION NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
