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
    ordering_mode TEXT NOT NULL DEFAULT 'strict',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ordered_jobs_chain_sequence UNIQUE (chain_id, sequence)
);

ALTER TABLE ordered_jobs ADD COLUMN IF NOT EXISTS ordering_mode TEXT NOT NULL DEFAULT 'strict';

-- Multi-tenancy optional unique (tenant + chain + seq)
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_chain_seq
ON ordered_jobs (tenant_id, chain_id, sequence)
WHERE tenant_id IS NOT NULL;

-- High-performance Partial Claim Index (C3, section 20)
CREATE INDEX IF NOT EXISTS idx_ordered_jobs_claimable
ON ordered_jobs (available_at ASC, created_at ASC)
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

-- LISTEN / NOTIFY trigger function for instant wakeup
CREATE OR REPLACE FUNCTION notify_ordered_job_event()
RETURNS trigger AS $$
BEGIN
  IF (NEW.status IN ('PENDING', 'RETRYING')) THEN
    PERFORM pg_notify('ordered_jobs', NEW.chain_id);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_ordered_job ON ordered_jobs;
CREATE TRIGGER trg_notify_ordered_job
AFTER INSERT OR UPDATE ON ordered_jobs
FOR EACH ROW EXECUTE FUNCTION notify_ordered_job_event();
