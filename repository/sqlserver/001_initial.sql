IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ordered_jobs')
BEGIN
    CREATE TABLE ordered_jobs (
        id               NVARCHAR(36) NOT NULL PRIMARY KEY,
        chain_id         NVARCHAR(255) NOT NULL,
        sequence         BIGINT NOT NULL,
        job_type         NVARCHAR(255) NOT NULL,
        payload          NVARCHAR(MAX) NOT NULL,
        status           NVARCHAR(50) NOT NULL,
        attempt          INT NOT NULL DEFAULT 0,
        max_attempts     INT NOT NULL DEFAULT 5,
        available_at     DATETIMEOFFSET NOT NULL DEFAULT GETUTCDATE(),
        deadline_at      DATETIMEOFFSET NULL,
        worker_id        NVARCHAR(255) NULL,
        lease_until      DATETIMEOFFSET NULL,
        lease_generation INT NOT NULL DEFAULT 0,
        last_error       NVARCHAR(MAX) NULL,
        tenant_id        NVARCHAR(255) NULL,
        idempotency_key  NVARCHAR(255) NULL,
        trace_id         NVARCHAR(255) NULL,
        created_at       DATETIMEOFFSET NOT NULL DEFAULT GETUTCDATE(),
        updated_at       DATETIMEOFFSET NOT NULL DEFAULT GETUTCDATE(),
        started_at       DATETIMEOFFSET NULL,
        completed_at     DATETIMEOFFSET NULL,
        failed_at        DATETIMEOFFSET NULL,

        CONSTRAINT uq_ordered_jobs_chain_sequence UNIQUE (chain_id, sequence)
    );
END;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_ordered_jobs_claimable')
BEGIN
    CREATE INDEX idx_ordered_jobs_claimable 
    ON ordered_jobs (available_at, created_at)
    INCLUDE (id, chain_id, sequence, job_type, status, attempt, max_attempts, worker_id, lease_until, lease_generation, idempotency_key, tenant_id, trace_id);
END;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_ordered_jobs_chain_seq')
BEGIN
    CREATE INDEX idx_ordered_jobs_chain_seq 
    ON ordered_jobs (chain_id, sequence);
END;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_ordered_jobs_idemp')
BEGIN
    CREATE INDEX idx_ordered_jobs_idemp 
    ON ordered_jobs (idempotency_key) 
    WHERE idempotency_key IS NOT NULL;
END;
