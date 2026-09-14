package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	orderedjob "github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/lifecycle"
)

//go:embed 001_initial.sql
var migrationFS embed.FS

type Repository struct {
	pool          *pgxpool.Pool
	allowGap      bool
	notifyChannel string
}

type Option func(*Repository)

func WithAllowGap(allow bool) Option {
	return func(r *Repository) { r.allowGap = allow }
}
func WithNotifyChannel(ch string) Option {
	return func(r *Repository) { r.notifyChannel = ch }
}

func New(pool *pgxpool.Pool, opts ...Option) *Repository {
	r := &Repository{
		pool:          pool,
		notifyChannel: "ordered_jobs",
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Repository) NotifyChannel() string { return r.notifyChannel }

func (r *Repository) Migrate(ctx context.Context) error {
	b, err := migrationFS.ReadFile("001_initial.sql")
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, string(b))
	return err
}

func (r *Repository) Enqueue(ctx context.Context, req orderedjob.EnqueueRequest) (orderedjob.Job, error) {
	var job orderedjob.Job
	err := withTx(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", req.ChainID); err != nil {
			return err
		}
		if existing, found, err := findExistingByIdempotencyKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if found {
			job = existing
			return nil
		}
		seq := req.Sequence
		if seq == 0 {
			var max *int64
			if err := tx.QueryRow(ctx, "SELECT MAX(sequence) FROM ordered_jobs WHERE chain_id=$1", req.ChainID).Scan(&max); err != nil {
				return err
			}
			if max == nil {
				seq = 1
			} else {
				seq = *max + 1
			}
		} else {
			var exists bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2)", req.ChainID, seq).Scan(&exists); err != nil {
				return err
			}
			if exists {
				return orderedjob.ErrDuplicateChainSeq
			}
			if seq > 1 && !r.allowGap {
				var predExists bool
				_ = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2)", req.ChainID, seq-1).Scan(&predExists)
				if !predExists {
					var max *int64
					_ = tx.QueryRow(ctx, "SELECT MAX(sequence) FROM ordered_jobs WHERE chain_id=$1", req.ChainID).Scan(&max)
					if max != nil && seq > *max+1 {
						return fmt.Errorf("%w: chain %s missing seq %d", orderedjob.ErrSequenceGap, req.ChainID, seq-1)
					}
					if max == nil && seq != 1 {
						return fmt.Errorf("%w: first sequence must be 1", orderedjob.ErrSequenceGap)
					}
				}
			}
		}
		status := lifecycle.StatePending
		if seq > 1 {
			var predStatus *string
			err := tx.QueryRow(ctx, "SELECT status FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2", req.ChainID, seq-1).Scan(&predStatus)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					status = lifecycle.StateBlocked
				} else {
					return err
				}
			} else if predStatus != nil && *predStatus != lifecycle.StateCompleted {
				status = lifecycle.StateBlocked
			}
		}
		payloadBytes, err := json.Marshal(req.Payload)
		if err != nil {
			return err
		}
		if len(payloadBytes) == 0 {
			payloadBytes = []byte("{}")
		}
		maxAttempts := req.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 5
		}
		availableAt := time.Now().UTC()
		if req.AvailableAt != nil {
			availableAt = *req.AvailableAt
		}
		id := uuid.New()
		var created orderedjob.Job
		err = tx.QueryRow(ctx, `
			INSERT INTO ordered_jobs (id, chain_id, sequence, job_type, payload, status, max_attempts, available_at, deadline_at, idempotency_key, tenant_id, trace_id, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW(),NOW())
			RETURNING id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(trace_id, '')
		`, id, req.ChainID, seq, req.Type, payloadBytes, status, maxAttempts, availableAt, req.DeadlineAt, nullable(req.IdempotencyKey), nullable(req.TenantID), nullable(req.TraceID)).Scan(
			&created.ID, &created.ChainID, &created.Sequence, &created.Type, &created.Payload, &created.Status, &created.Attempt, &created.MaxAttempts, &created.AvailableAt, &created.DeadlineAt, &created.WorkerID, &created.LeaseUntil, &created.LeaseGeneration, &created.CreatedAt, &created.UpdatedAt, &created.IdempotencyKey, &created.TenantID, &created.TraceID,
		)
		if err != nil {
			return err
		}
		job = created
		return nil
	})
	if err != nil {
		return orderedjob.Job{}, err
	}
	_ = r.notify(ctx, job.ChainID)
	return job, nil
}

func (r *Repository) EnqueueBatch(ctx context.Context, reqs []orderedjob.EnqueueRequest) ([]orderedjob.Job, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	var out []orderedjob.Job
	err := withTx(ctx, r.pool, func(tx pgx.Tx) error {
		chains := distinctChains(reqs)
		for _, ch := range chains {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", ch); err != nil {
				return err
			}
		}
		for _, req := range reqs {
			created, err := r.enqueueSingleInTx(ctx, tx, req, out)
			if err != nil {
				return err
			}
			out = append(out, created)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, j := range out {
		_ = r.notify(ctx, j.ChainID)
	}
	return out, nil
}

func (r *Repository) enqueueSingleInTx(ctx context.Context, tx pgx.Tx, req orderedjob.EnqueueRequest, previousInBatch []orderedjob.Job) (orderedjob.Job, error) {
	if existing, found, err := findExistingByIdempotencyKey(ctx, tx, req.IdempotencyKey); err != nil {
		return orderedjob.Job{}, err
	} else if found {
		return existing, nil
	}
	if req.IdempotencyKey != "" {
		for _, o := range previousInBatch {
			if o.IdempotencyKey == req.IdempotencyKey {
				return o, nil
			}
		}
	}
	seq := req.Sequence
	if seq == 0 {
		var max *int64
		if err := tx.QueryRow(ctx, "SELECT MAX(sequence) FROM ordered_jobs WHERE chain_id=$1", req.ChainID).Scan(&max); err != nil {
			return orderedjob.Job{}, err
		}
		if max == nil {
			seq = 1
		} else {
			seq = *max + 1
		}
	} else {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2)", req.ChainID, seq).Scan(&exists); err != nil {
			return orderedjob.Job{}, err
		}
		if exists {
			return orderedjob.Job{}, orderedjob.ErrDuplicateChainSeq
		}
	}
	status := lifecycle.StatePending
	if seq > 1 {
		var predStatus *string
		err := tx.QueryRow(ctx, "SELECT status FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2", req.ChainID, seq-1).Scan(&predStatus)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return orderedjob.Job{}, err
		}
		if predStatus == nil {
			for _, o := range previousInBatch {
				if o.ChainID == req.ChainID && o.Sequence == seq-1 {
					s := o.Status
					predStatus = &s
					break
				}
			}
		}
		if predStatus == nil || *predStatus != lifecycle.StateCompleted {
			status = lifecycle.StateBlocked
		}
	}
	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return orderedjob.Job{}, err
	}
	if len(payloadBytes) == 0 {
		payloadBytes = []byte("{}")
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	availableAt := time.Now().UTC()
	if req.AvailableAt != nil {
		availableAt = *req.AvailableAt
	}
	id := uuid.New()
	var created orderedjob.Job
	err = tx.QueryRow(ctx, `
		INSERT INTO ordered_jobs (id, chain_id, sequence, job_type, payload, status, max_attempts, available_at, deadline_at, idempotency_key, tenant_id, trace_id, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW(),NOW())
		RETURNING id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(trace_id, '')
	`, id, req.ChainID, seq, req.Type, payloadBytes, status, maxAttempts, availableAt, req.DeadlineAt, nullable(req.IdempotencyKey), nullable(req.TenantID), nullable(req.TraceID)).Scan(
		&created.ID, &created.ChainID, &created.Sequence, &created.Type, &created.Payload, &created.Status, &created.Attempt, &created.MaxAttempts, &created.AvailableAt, &created.DeadlineAt, &created.WorkerID, &created.LeaseUntil, &created.LeaseGeneration, &created.CreatedAt, &created.UpdatedAt, &created.IdempotencyKey, &created.TenantID, &created.TraceID,
	)
	if err != nil {
		return orderedjob.Job{}, err
	}
	return created, nil
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (orderedjob.Job, error) {
	var j orderedjob.Job
	err := r.pool.QueryRow(ctx, `SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '') FROM ordered_jobs WHERE id=$1`, id).Scan(
		&j.ID, &j.ChainID, &j.Sequence, &j.Type, &j.Payload, &j.Status, &j.Attempt, &j.MaxAttempts, &j.AvailableAt, &j.DeadlineAt, &j.WorkerID, &j.LeaseUntil, &j.LeaseGeneration, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt, &j.FailedAt, &j.IdempotencyKey, &j.TenantID, &j.LastError, &j.TraceID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
		return orderedjob.Job{}, err
	}
	return j, nil
}

func (r *Repository) GetByChainSeq(ctx context.Context, chainID string, seq int64) (orderedjob.Job, error) {
	var j orderedjob.Job
	err := r.pool.QueryRow(ctx, `SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '') FROM ordered_jobs WHERE chain_id=$1 AND sequence=$2`, chainID, seq).Scan(
		&j.ID, &j.ChainID, &j.Sequence, &j.Type, &j.Payload, &j.Status, &j.Attempt, &j.MaxAttempts, &j.AvailableAt, &j.DeadlineAt, &j.WorkerID, &j.LeaseUntil, &j.LeaseGeneration, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt, &j.FailedAt, &j.IdempotencyKey, &j.TenantID, &j.LastError, &j.TraceID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
		return orderedjob.Job{}, err
	}
	return j, nil
}

func (r *Repository) Claim(ctx context.Context, workerID string, lease time.Duration) (orderedjob.Job, error) {
	var job orderedjob.Job
	leaseSec := int64(lease.Seconds())
	if leaseSec <= 0 {
		leaseSec = 60
	}
	err := withTx(ctx, r.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
WITH candidate AS (
	SELECT j.id
	FROM ordered_jobs j
	WHERE j.status IN ('PENDING','RETRYING')
	  AND j.available_at <= NOW()
	  AND (j.sequence = 1 OR EXISTS (
		SELECT 1 FROM ordered_jobs p
		WHERE p.chain_id = j.chain_id AND p.sequence = j.sequence - 1 AND p.status = 'COMPLETED'
	  ))
	ORDER BY j.available_at ASC, j.created_at ASC
	FOR UPDATE SKIP LOCKED
	LIMIT 1
)
UPDATE ordered_jobs o
SET status = 'PROCESSING',
    worker_id = $1,
    lease_until = NOW() + ($2 * INTERVAL '1 second'),
    lease_generation = o.lease_generation + 1,
    attempt = o.attempt + 1,
    started_at = COALESCE(o.started_at, NOW()),
    updated_at = NOW()
FROM candidate c
WHERE o.id = c.id
RETURNING o.id, o.chain_id, o.sequence, o.job_type, o.payload, o.status, o.attempt, o.max_attempts, o.available_at, o.deadline_at, COALESCE(o.worker_id, ''), o.lease_until, o.lease_generation, o.created_at, o.updated_at, o.started_at, o.completed_at, o.failed_at, COALESCE(o.idempotency_key, ''), COALESCE(o.tenant_id, ''), COALESCE(o.last_error, ''), COALESCE(o.trace_id, '')
`, workerID, leaseSec)
		err := row.Scan(
			&job.ID, &job.ChainID, &job.Sequence, &job.Type, &job.Payload, &job.Status, &job.Attempt, &job.MaxAttempts, &job.AvailableAt, &job.DeadlineAt, &job.WorkerID, &job.LeaseUntil, &job.LeaseGeneration, &job.CreatedAt, &job.UpdatedAt, &job.StartedAt, &job.CompletedAt, &job.FailedAt, &job.IdempotencyKey, &job.TenantID, &job.LastError, &job.TraceID,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orderedjob.ErrNotFound
			}
			return err
		}
		return nil
	})
	return job, err
}

func (r *Repository) Complete(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	return withTx(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE ordered_jobs
			SET status='COMPLETED', completed_at=NOW(), updated_at=NOW(), lease_until=NULL
			WHERE id=$1 AND worker_id=$2 AND lease_generation=$3 AND status='PROCESSING'
		`, id, workerID, leaseGen)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return orderedjob.ErrLeaseConflict
		}
		var chainID string
		var seq int64
		if err := tx.QueryRow(ctx, "SELECT chain_id, sequence FROM ordered_jobs WHERE id=$1", id).Scan(&chainID, &seq); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE ordered_jobs
			SET status='PENDING', updated_at=NOW()
			WHERE chain_id=$1 AND sequence=$2 AND status='BLOCKED'
		`, chainID, seq+1)
		if err != nil {
			return err
		}
		_ = r.notifyTx(ctx, tx, chainID)
		return nil
	})
}

func (r *Repository) Fail(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, terminal bool) error {
	return withTx(ctx, r.pool, func(tx pgx.Tx) error {
		status := lifecycle.StateFailed
		tag, err := tx.Exec(ctx, `
			UPDATE ordered_jobs
			SET status=$1, failed_at=NOW(), updated_at=NOW(), last_error=$2, lease_until=NULL
			WHERE id=$3 AND worker_id=$4 AND lease_generation=$5 AND status='PROCESSING'
		`, status, errMsg, id, workerID, leaseGen)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return orderedjob.ErrLeaseConflict
		}
		return nil
	})
}

func (r *Repository) Retry(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, nextAvailable time.Time) error {
	return withTx(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE ordered_jobs
			SET status='RETRYING', available_at=$1, updated_at=NOW(), last_error=$2, lease_until=NULL
			WHERE id=$3 AND worker_id=$4 AND lease_generation=$5 AND status='PROCESSING'
		`, nextAvailable, errMsg, id, workerID, leaseGen)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return orderedjob.ErrLeaseConflict
		}
		return nil
	})
}

func (r *Repository) Heartbeat(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, lease time.Duration) error {
	leaseSec := int64(lease.Seconds())
	if leaseSec <= 0 {
		leaseSec = 60
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE ordered_jobs
		SET lease_until=NOW()+($1 * INTERVAL '1 second'), updated_at=NOW()
		WHERE id=$2 AND worker_id=$3 AND lease_generation=$4 AND status='PROCESSING'
	`, leaseSec, id, workerID, leaseGen)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) RequestCancel(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE ordered_jobs
		SET status='CANCEL_REQUESTED', updated_at=NOW()
		WHERE id=$1 AND status IN ('PENDING','RETRYING','BLOCKED','PROCESSING')
	`, id)
	return err
}

func (r *Repository) ConfirmCancel(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE ordered_jobs
		SET status='CANCELLED', updated_at=NOW(), lease_until=NULL
		WHERE id=$1 AND worker_id=$2 AND lease_generation=$3
	`, id, workerID, leaseGen)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) RecoverStale(ctx context.Context, limit int, retryPolicy func(attempt int) time.Duration) (int, error) {
	var recovered int
	err := withTx(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, attempt FROM ordered_jobs
			WHERE status='PROCESSING' AND lease_until < NOW()
			ORDER BY lease_until ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		var list []struct {
			id      uuid.UUID
			attempt int
		}
		for rows.Next() {
			var s struct {
				id      uuid.UUID
				attempt int
			}
			if err := rows.Scan(&s.id, &s.attempt); err != nil {
				return err
			}
			list = append(list, s)
		}
		for _, s := range list {
			delay := time.Second
			if retryPolicy != nil {
				delay = retryPolicy(s.attempt)
			}
			next := time.Now().UTC().Add(delay)
			_, err := tx.Exec(ctx, `
				UPDATE ordered_jobs
				SET status='RETRYING', available_at=$1, updated_at=NOW(), last_error='stale lease recovered', lease_until=NULL
				WHERE id=$2 AND status='PROCESSING' AND lease_until < NOW()
			`, next, s.id)
			if err != nil {
				return err
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}

func (r *Repository) PromoteNext(ctx context.Context, chainID string, completedSeq int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE ordered_jobs
		SET status='PENDING', updated_at=NOW()
		WHERE chain_id=$1 AND sequence=$2 AND status='BLOCKED'
	`, chainID, completedSeq+1)
	return err
}

func (r *Repository) ListPendingChains(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT DISTINCT chain_id FROM ordered_jobs WHERE status='PENDING'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (r *Repository) Listen(ctx context.Context, callback func(chainID string)) error {
	if r.notifyChannel == "" {
		return nil
	}
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{r.notifyChannel}.Sanitize())
	if err != nil {
		return err
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if notification != nil {
			callback(notification.Payload)
		}
	}
}

func (r *Repository) ReplayJob(ctx context.Context, id uuid.UUID) error {
	return withTx(ctx, r.pool, func(tx pgx.Tx) error {
		var chainID string
		err := tx.QueryRow(ctx, `
			UPDATE ordered_jobs
			SET status='PENDING', attempt=0, last_error=NULL, available_at=NOW(), updated_at=NOW(), lease_until=NULL
			WHERE id=$1 AND status IN ('FAILED', 'CANCELLED', 'DEAD_LETTERED')
			RETURNING chain_id
		`, id).Scan(&chainID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orderedjob.ErrNotFound
			}
			return err
		}
		_ = r.notifyTx(ctx, tx, chainID)
		return nil
	})
}

func (r *Repository) SkipJob(ctx context.Context, id uuid.UUID) error {
	return withTx(ctx, r.pool, func(tx pgx.Tx) error {
		var chainID string
		var seq int64
		err := tx.QueryRow(ctx, `
			UPDATE ordered_jobs
			SET status='COMPLETED', completed_at=NOW(), updated_at=NOW(), lease_until=NULL
			WHERE id=$1 AND status IN ('FAILED', 'CANCELLED', 'DEAD_LETTERED')
			RETURNING chain_id, sequence
		`, id).Scan(&chainID, &seq)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orderedjob.ErrNotFound
			}
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE ordered_jobs
			SET status='PENDING', updated_at=NOW()
			WHERE chain_id=$1 AND sequence=$2 AND status='BLOCKED'
		`, chainID, seq+1)
		if err != nil {
			return err
		}
		_ = r.notifyTx(ctx, tx, chainID)
		return nil
	})
}

func (r *Repository) notify(ctx context.Context, chainID string) error {
	_, err := r.pool.Exec(ctx, "SELECT pg_notify($1, $2)", r.notifyChannel, chainID)
	return err
}
func (r *Repository) notifyTx(ctx context.Context, tx pgx.Tx, chainID string) error {
	_, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", r.notifyChannel, chainID)
	return err
}

func withTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func findExistingByIdempotencyKey(ctx context.Context, tx pgx.Tx, key string) (orderedjob.Job, bool, error) {
	if key == "" {
		return orderedjob.Job{}, false, nil
	}
	var existing orderedjob.Job
	err := tx.QueryRow(ctx, `
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE idempotency_key = $1
	`, key).Scan(
		&existing.ID, &existing.ChainID, &existing.Sequence, &existing.Type, &existing.Payload, &existing.Status, &existing.Attempt, &existing.MaxAttempts, &existing.AvailableAt, &existing.DeadlineAt, &existing.WorkerID, &existing.LeaseUntil, &existing.LeaseGeneration, &existing.CreatedAt, &existing.UpdatedAt, &existing.IdempotencyKey, &existing.TenantID, &existing.TraceID,
	)
	if err == nil {
		return existing, true, nil
	} else if errors.Is(err, pgx.ErrNoRows) {
		return orderedjob.Job{}, false, nil
	}
	return orderedjob.Job{}, false, err
}

func scanJob(s pgx.Row) (orderedjob.Job, error) {
	var j orderedjob.Job
	err := s.Scan(
		&j.ID, &j.ChainID, &j.Sequence, &j.Type, &j.Payload, &j.Status, &j.Attempt, &j.MaxAttempts,
		&j.AvailableAt, &j.DeadlineAt, &j.WorkerID, &j.LeaseUntil, &j.LeaseGeneration,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt, &j.FailedAt,
		&j.IdempotencyKey, &j.TenantID, &j.LastError, &j.TraceID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
		return orderedjob.Job{}, err
	}
	return j, nil
}

func distinctChains(reqs []orderedjob.EnqueueRequest) []string {
	set := map[string]struct{}{}
	for _, r := range reqs {
		set[r.ChainID] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func (r *Repository) GetStats(ctx context.Context) (orderedjob.Stats, error) {
	var stats orderedjob.Stats
	rows, err := r.pool.Query(ctx, `
		SELECT status, COUNT(*)
		FROM ordered_jobs
		GROUP BY status
	`)
	if err != nil {
		return stats, err
	}
	defer rows.Close()

	for rows.Next() {
		var st string
		var cnt int64
		if err := rows.Scan(&st, &cnt); err != nil {
			return stats, err
		}
		stats.Total += cnt
		switch st {
		case lifecycle.StatePending:
			stats.Pending = cnt
		case lifecycle.StateBlocked:
			stats.Blocked = cnt
		case lifecycle.StateProcessing:
			stats.Processing = cnt
		case lifecycle.StateCompleted:
			stats.Completed = cnt
		case lifecycle.StateRetrying:
			stats.Retrying = cnt
		case lifecycle.StateFailed:
			stats.Failed = cnt
		case lifecycle.StateDeadLettered:
			stats.DeadLettered = cnt
		case lifecycle.StateCancelRequested:
			stats.CancelRequested = cnt
		case lifecycle.StateCancelled:
			stats.Cancelled = cnt
		}
	}

	err = r.pool.QueryRow(ctx, `SELECT COUNT(DISTINCT chain_id) FROM ordered_jobs`).Scan(&stats.ActiveChains)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func (r *Repository) ListJobs(ctx context.Context, filter orderedjob.JobFilter) ([]orderedjob.Job, int64, error) {
	whereClauses := []string{"1=1"}
	args := []any{}
	argIdx := 1

	if filter.ChainID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("chain_id = $%d", argIdx))
		args = append(args, filter.ChainID)
		argIdx++
	}
	if filter.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, filter.Status)
		argIdx++
	}
	if filter.JobType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("job_type = $%d", argIdx))
		args = append(args, filter.JobType)
		argIdx++
	}
	if filter.Search != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(id::text ILIKE $%d OR idempotency_key ILIKE $%d OR trace_id ILIKE $%d)", argIdx, argIdx, argIdx))
		args = append(args, "%"+filter.Search+"%")
		argIdx++
	}

	whereStmt := strings.Join(whereClauses, " AND ")

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM ordered_jobs WHERE %s", whereStmt)
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	orderByCol := "created_at"
	switch strings.ToLower(filter.OrderBy) {
	case "sequence":
		orderByCol = "sequence"
	case "available_at":
		orderByCol = "available_at"
	case "job_type":
		orderByCol = "job_type"
	case "status":
		orderByCol = "status"
	case "created_at":
		orderByCol = "created_at"
	}

	orderDir := "DESC"
	if strings.EqualFold(filter.OrderDir, "asc") {
		orderDir = "ASC"
	}

	dataQuery := fmt.Sprintf(`
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE %s
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereStmt, orderByCol, orderDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []orderedjob.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, j)
	}

	return jobs, total, nil
}

func (r *Repository) ListChains(ctx context.Context, filter orderedjob.ChainFilter) ([]orderedjob.ChainSummary, int64, error) {
	var whereClause string
	var args []any
	argIdx := 1

	if filter.Search != "" {
		whereClause = fmt.Sprintf("WHERE chain_id ILIKE $%d", argIdx)
		args = append(args, "%"+filter.Search+"%")
		argIdx++
	}

	countQuery := fmt.Sprintf("SELECT COUNT(DISTINCT chain_id) FROM ordered_jobs %s", whereClause)
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	orderByCol := "chain_id"
	switch strings.ToLower(filter.OrderBy) {
	case "total_jobs":
		orderByCol = "total_jobs"
	case "max_sequence":
		orderByCol = "max_sequence"
	case "pending_jobs":
		orderByCol = "pending_jobs"
	case "failed_jobs":
		orderByCol = "failed_jobs"
	case "chain_id":
		orderByCol = "chain_id"
	}

	orderDir := "ASC"
	if strings.EqualFold(filter.OrderDir, "desc") {
		orderDir = "DESC"
	}

	dataQuery := fmt.Sprintf(`
		SELECT 
			chain_id,
			COUNT(*) as total_jobs,
			MAX(sequence) as max_sequence,
			COUNT(*) FILTER (WHERE status IN ('PENDING', 'PROCESSING', 'RETRYING')) as pending_jobs,
			COUNT(*) FILTER (WHERE status IN ('FAILED', 'DEAD_LETTERED')) as failed_jobs
		FROM ordered_jobs
		%s
		GROUP BY chain_id
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereClause, orderByCol, orderDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var summaries []orderedjob.ChainSummary
	for rows.Next() {
		var cs orderedjob.ChainSummary
		if err := rows.Scan(&cs.ChainID, &cs.TotalJobs, &cs.MaxSequence, &cs.PendingJobs, &cs.FailedJobs); err != nil {
			return nil, 0, err
		}

		_ = r.pool.QueryRow(ctx, "SELECT status FROM ordered_jobs WHERE chain_id = $1 AND sequence = $2", cs.ChainID, cs.MaxSequence).Scan(&cs.LatestStatus)

		summaries = append(summaries, cs)
	}

	return summaries, total, nil
}
