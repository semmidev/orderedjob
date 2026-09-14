-- Ordered Job Engine — initial schema
-- Compatible with PostgreSQL 13+
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS ordered_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline_at TIMESTAMPTZ,
    worker_id TEXT,
    lease_until TIMESTAMPTZ,
    lease_generation INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    idempotency_key TEXT,
    last_error TEXT,
    tenant_id TEXT,
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ordered_jobs_chain_sequence UNIQUE (chain_id, sequence)
);

-- Multi-tenancy optional unique (tenant + chain + seq)
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_chain_seq
ON ordered_jobs (tenant_id, chain_id, sequence)
WHERE tenant_id IS NOT NULL;

-- Claim index (C3, section 20)
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_claim
ON ordered_jobs (status, available_at, created_at)
WHERE status IN ('PENDING','RETRYING');

-- Dedicated Scheduled / Delayed Jobs Index
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_scheduled
ON ordered_jobs (available_at ASC, chain_id, sequence)
WHERE status IN ('PENDING', 'RETRYING');

-- Chain lookup for eligibility & promotion
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_chain_seq
ON ordered_jobs (chain_id, sequence);

-- Lease recovery
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_lease
ON ordered_jobs (lease_until)
WHERE status = 'PROCESSING';

-- Idempotency
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_idempotency
ON ordered_jobs (idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Chain blocked detection / observability
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_status_chain
ON ordered_jobs (chain_id, status);
