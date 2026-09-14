package sqlserver

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/microsoft/go-mssqldb" // SQL Server driver
	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/lifecycle"
)

//go:embed 001_initial.sql
var migrationFS embed.FS

type Repository struct {
	db       *sql.DB
	allowGap bool
}

type Option func(*Repository)

func WithAllowGap(allow bool) Option {
	return func(r *Repository) { r.allowGap = allow }
}

func New(db *sql.DB, opts ...Option) *Repository {
	r := &Repository{db: db}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Repository) NotifyChannel() string { return "" }

func (r *Repository) Listen(ctx context.Context, callback func(chainID string)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *Repository) Enqueue(ctx context.Context, req orderedjob.EnqueueRequest) (orderedjob.Job, error) {
	if req.ChainID == "" {
		return orderedjob.Job{}, fmt.Errorf("chain_id required")
	}
	if req.Type == "" {
		return orderedjob.Job{}, fmt.Errorf("job type required")
	}

	var job orderedjob.Job
	err := withTx(ctx, r.db, func(tx *sql.Tx) error {
		if err := lockAppChain(ctx, tx, req.ChainID); err != nil {
			return err
		}
		if existing, found, err := findExistingByIdempotencyKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if found {
			job = existing
			return nil
		}
		created, err := r.enqueueSingleInTx(ctx, tx, req, nil)
		if err != nil {
			return err
		}
		job = created
		return nil
	})
	if err != nil {
		return orderedjob.Job{}, err
	}
	return job, nil
}

func (r *Repository) EnqueueBatch(ctx context.Context, reqs []orderedjob.EnqueueRequest) ([]orderedjob.Job, error) {
	for i, req := range reqs {
		if req.ChainID == "" {
			return nil, fmt.Errorf("request %d: chain_id required", i)
		}
		if req.Type == "" {
			return nil, fmt.Errorf("request %d: job type required", i)
		}
	}

	chains := distinctChains(reqs)
	var out []orderedjob.Job

	err := withTx(ctx, r.db, func(tx *sql.Tx) error {
		for _, ch := range chains {
			if err := lockAppChain(ctx, tx, ch); err != nil {
				return err
			}
		}

		for _, req := range reqs {
			job, err := r.enqueueSingleInTx(ctx, tx, req, out)
			if err != nil {
				return err
			}
			out = append(out, job)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repository) resolveSequence(ctx context.Context, tx *sql.Tx, req orderedjob.EnqueueRequest) (int64, error) {
	seq := req.Sequence
	if seq == 0 {
		var max sql.NullInt64
		if err := tx.QueryRowContext(ctx, "SELECT MAX(sequence) FROM ordered_jobs WITH (UPDLOCK) WHERE chain_id = @p1", req.ChainID).Scan(&max); err != nil {
			return 0, err
		}
		if !max.Valid {
			return 1, nil
		}
		return max.Int64 + 1, nil
	}

	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT CASE WHEN EXISTS(SELECT 1 FROM ordered_jobs WITH (UPDLOCK) WHERE chain_id = @p1 AND sequence = @p2) THEN 1 ELSE 0 END", req.ChainID, seq).Scan(&exists); err != nil {
		return 0, err
	}
	if exists == 1 {
		return 0, orderedjob.ErrDuplicateChainSeq
	}
	if seq > 1 && !r.allowGap {
		var predExists int
		_ = tx.QueryRowContext(ctx, "SELECT CASE WHEN EXISTS(SELECT 1 FROM ordered_jobs WHERE chain_id = @p1 AND sequence = @p2) THEN 1 ELSE 0 END", req.ChainID, seq-1).Scan(&predExists)
		if predExists == 0 {
			var max sql.NullInt64
			_ = tx.QueryRowContext(ctx, "SELECT MAX(sequence) FROM ordered_jobs WHERE chain_id = @p1", req.ChainID).Scan(&max)
			if max.Valid && seq > max.Int64+1 {
				return 0, fmt.Errorf("%w: chain %s missing seq %d", orderedjob.ErrSequenceGap, req.ChainID, seq-1)
			}
			if !max.Valid && seq != 1 {
				return 0, fmt.Errorf("%w: first sequence must be 1", orderedjob.ErrSequenceGap)
			}
		}
	}
	return seq, nil
}

func resolveInitialStatus(ctx context.Context, tx *sql.Tx, chainID string, seq int64, previousInBatch []orderedjob.Job) (string, error) {
	if seq <= 1 {
		return lifecycle.StatePending, nil
	}

	var predStatus sql.NullString
	err := tx.QueryRowContext(ctx, "SELECT status FROM ordered_jobs WHERE chain_id = @p1 AND sequence = @p2", chainID, seq-1).Scan(&predStatus)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if !predStatus.Valid {
		for _, o := range previousInBatch {
			if o.ChainID == chainID && o.Sequence == seq-1 {
				s := o.Status
				predStatus = sql.NullString{String: s, Valid: true}
				break
			}
		}
	}
	if !predStatus.Valid || predStatus.String != lifecycle.StateCompleted {
		return lifecycle.StateBlocked, nil
	}
	return lifecycle.StatePending, nil
}

func (r *Repository) enqueueSingleInTx(ctx context.Context, tx *sql.Tx, req orderedjob.EnqueueRequest, previousInBatch []orderedjob.Job) (orderedjob.Job, error) {
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

	seq, err := r.resolveSequence(ctx, tx, req)
	if err != nil {
		return orderedjob.Job{}, err
	}

	status, err := resolveInitialStatus(ctx, tx, req.ChainID, seq, previousInBatch)
	if err != nil {
		return orderedjob.Job{}, err
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

	query := `
		INSERT INTO ordered_jobs (id, chain_id, sequence, job_type, payload, status, max_attempts, available_at, deadline_at, idempotency_key, tenant_id, trace_id, created_at, updated_at)
		OUTPUT inserted.id, inserted.chain_id, inserted.sequence, inserted.job_type, inserted.payload, inserted.status, inserted.attempt, inserted.max_attempts, inserted.available_at, inserted.deadline_at, COALESCE(inserted.worker_id, ''), inserted.lease_until, inserted.lease_generation, inserted.created_at, inserted.updated_at, COALESCE(inserted.idempotency_key, ''), COALESCE(inserted.tenant_id, ''), COALESCE(inserted.trace_id, '')
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, GETUTCDATE(), GETUTCDATE())
	`

	var created orderedjob.Job
	var payloadStr string
	var idStr string
	err = tx.QueryRowContext(ctx, query,
		id.String(), req.ChainID, seq, req.Type, string(payloadBytes), status, maxAttempts, availableAt, req.DeadlineAt, nullable(req.IdempotencyKey), nullable(req.TenantID), nullable(req.TraceID),
	).Scan(
		&idStr, &created.ChainID, &created.Sequence, &created.Type, &payloadStr, &created.Status, &created.Attempt, &created.MaxAttempts, &created.AvailableAt, &created.DeadlineAt, &created.WorkerID, &created.LeaseUntil, &created.LeaseGeneration, &created.CreatedAt, &created.UpdatedAt, &created.IdempotencyKey, &created.TenantID, &created.TraceID,
	)
	if err != nil {
		return orderedjob.Job{}, err
	}
	created.ID, _ = uuid.Parse(idStr)
	created.Payload = json.RawMessage(payloadStr)
	return created, nil
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (orderedjob.Job, error) {
	query := `
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE id = @p1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())
	return scanJob(row)
}

func (r *Repository) GetByChainSeq(ctx context.Context, chainID string, seq int64) (orderedjob.Job, error) {
	query := `
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE chain_id = @p1 AND sequence = @p2
	`
	row := r.db.QueryRowContext(ctx, query, chainID, seq)
	return scanJob(row)
}

func (r *Repository) Claim(ctx context.Context, workerID string, lease time.Duration) (orderedjob.Job, error) {
	leaseSec := int(lease.Seconds())
	if leaseSec <= 0 {
		leaseSec = 60
	}

	query := `
		WITH Target AS (
			SELECT TOP (1) id
			FROM ordered_jobs WITH (UPDLOCK, READPAST)
			WHERE status IN ('PENDING', 'RETRYING')
			  AND available_at <= GETUTCDATE()
			  AND (sequence = 1 OR EXISTS (
				  SELECT 1 FROM ordered_jobs p WITH (NOLOCK)
				  WHERE p.chain_id = ordered_jobs.chain_id
				    AND p.sequence = ordered_jobs.sequence - 1
				    AND p.status = 'COMPLETED'
			  ))
			ORDER BY available_at ASC, created_at ASC
		)
		UPDATE ordered_jobs
		SET status = 'PROCESSING',
		    worker_id = @p1,
		    lease_until = DATEADD(second, @p2, GETUTCDATE()),
		    lease_generation = lease_generation + 1,
		    attempt = attempt + 1,
		    started_at = ISNULL(started_at, GETUTCDATE()),
		    updated_at = GETUTCDATE()
		OUTPUT inserted.id, inserted.chain_id, inserted.sequence, inserted.job_type, inserted.payload, inserted.status, inserted.attempt, inserted.max_attempts, inserted.available_at, inserted.deadline_at, COALESCE(inserted.worker_id, ''), inserted.lease_until, inserted.lease_generation, inserted.created_at, inserted.updated_at, inserted.started_at, inserted.completed_at, inserted.failed_at, COALESCE(inserted.idempotency_key, ''), COALESCE(inserted.tenant_id, ''), COALESCE(inserted.last_error, ''), COALESCE(inserted.trace_id, '')
		FROM ordered_jobs WITH (READPAST)
		INNER JOIN Target ON ordered_jobs.id = Target.id;
	`

	row := r.db.QueryRowContext(ctx, query, workerID, leaseSec)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
		return orderedjob.Job{}, err
	}
	return job, nil
}

func (r *Repository) Complete(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	var chainID string
	var seq int64

	err := withTx(ctx, r.db, func(tx *sql.Tx) error {
		query := `
			UPDATE ordered_jobs
			SET status = 'COMPLETED', completed_at = GETUTCDATE(), updated_at = GETUTCDATE()
			WHERE id = @p1 AND worker_id = @p2 AND lease_generation = @p3 AND status = 'PROCESSING'
		`
		res, err := tx.ExecContext(ctx, query, id.String(), workerID, leaseGen)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return orderedjob.ErrLeaseConflict
		}

		if err := tx.QueryRowContext(ctx, "SELECT chain_id, sequence FROM ordered_jobs WHERE id = @p1", id.String()).Scan(&chainID, &seq); err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE ordered_jobs
			SET status = 'PENDING', updated_at = GETUTCDATE()
			WHERE chain_id = @p1 AND sequence = @p2 AND status = 'BLOCKED'
		`, chainID, seq+1)
		return err
	})
	return err
}

func (r *Repository) Fail(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, terminal bool) error {
	status := lifecycle.StateFailed
	if terminal {
		status = lifecycle.StateDeadLettered
	}

	query := `
		UPDATE ordered_jobs
		SET status = @p1, failed_at = GETUTCDATE(), last_error = @p2, updated_at = GETUTCDATE()
		WHERE id = @p3 AND worker_id = @p4 AND lease_generation = @p5 AND status = 'PROCESSING'
	`
	res, err := r.db.ExecContext(ctx, query, status, errMsg, id.String(), workerID, leaseGen)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) Retry(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, nextAvailable time.Time) error {
	query := `
		UPDATE ordered_jobs
		SET status = 'RETRYING', available_at = @p1, last_error = @p2, updated_at = GETUTCDATE()
		WHERE id = @p3 AND worker_id = @p4 AND lease_generation = @p5 AND status = 'PROCESSING'
	`
	res, err := r.db.ExecContext(ctx, query, nextAvailable, errMsg, id.String(), workerID, leaseGen)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) Heartbeat(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, lease time.Duration) error {
	leaseSec := int(lease.Seconds())
	if leaseSec <= 0 {
		leaseSec = 60
	}

	query := `
		UPDATE ordered_jobs
		SET lease_until = DATEADD(second, @p1, GETUTCDATE()), updated_at = GETUTCDATE()
		WHERE id = @p2 AND worker_id = @p3 AND lease_generation = @p4 AND status = 'PROCESSING'
	`
	res, err := r.db.ExecContext(ctx, query, leaseSec, id.String(), workerID, leaseGen)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) RequestCancel(ctx context.Context, id uuid.UUID) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE ordered_jobs
		SET status = 'CANCEL_REQUESTED', updated_at = GETUTCDATE()
		WHERE id = @p1 AND status IN ('PENDING', 'BLOCKED', 'PROCESSING', 'RETRYING')
	`, id.String())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrNotFound
	}
	return nil
}

func (r *Repository) ConfirmCancel(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE ordered_jobs
		SET status = 'CANCELLED', updated_at = GETUTCDATE()
		WHERE id = @p1 AND worker_id = @p2 AND lease_generation = @p3 AND status = 'CANCEL_REQUESTED'
	`, id.String(), workerID, leaseGen)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrLeaseConflict
	}
	return nil
}

func (r *Repository) RecoverStale(ctx context.Context, limit int, retryPolicy func(attempt int) time.Duration) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT TOP (@p1) id, attempt, max_attempts
		FROM ordered_jobs WITH (UPDLOCK, READPAST)
		WHERE status = 'PROCESSING' AND lease_until < GETUTCDATE()
	`, limit)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	type staleItem struct {
		id          string
		attempt     int
		maxAttempts int
	}
	var items []staleItem

	for rows.Next() {
		var it staleItem
		if err := rows.Scan(&it.id, &it.attempt, &it.maxAttempts); err != nil {
			return 0, err
		}
		items = append(items, it)
	}

	recovered := 0
	for _, it := range items {
		var nextStatus string
		var nextAvailable time.Time
		if retryPolicy != nil && it.attempt < it.maxAttempts {
			nextStatus = lifecycle.StateRetrying
			nextAvailable = time.Now().UTC().Add(retryPolicy(it.attempt))
		} else {
			nextStatus = lifecycle.StatePending
			nextAvailable = time.Now().UTC()
		}

		res, err := r.db.ExecContext(ctx, `
			UPDATE ordered_jobs
			SET status = @p1, available_at = @p2, worker_id = NULL, lease_until = NULL, lease_generation = lease_generation + 1, updated_at = GETUTCDATE()
			WHERE id = @p3 AND status = 'PROCESSING' AND lease_until < GETUTCDATE()
		`, nextStatus, nextAvailable, it.id)
		if err != nil {
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			recovered++
		}
	}
	return recovered, nil
}

func (r *Repository) ReplayJob(ctx context.Context, id uuid.UUID) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE ordered_jobs
		SET status = 'PENDING', attempt = 0, available_at = GETUTCDATE(), updated_at = GETUTCDATE()
		WHERE id = @p1 AND status IN ('FAILED', 'DEAD_LETTERED', 'CANCELLED')
	`, id.String())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return orderedjob.ErrNotFound
	}
	return nil
}

func (r *Repository) SkipJob(ctx context.Context, id uuid.UUID) error {
	var chainID string
	var seq int64

	err := withTx(ctx, r.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE ordered_jobs
			SET status = 'COMPLETED', updated_at = GETUTCDATE()
			WHERE id = @p1 AND status IN ('FAILED', 'DEAD_LETTERED', 'CANCELLED', 'BLOCKED')
		`, id.String())
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return orderedjob.ErrNotFound
		}

		if err := tx.QueryRowContext(ctx, "SELECT chain_id, sequence FROM ordered_jobs WHERE id = @p1", id.String()).Scan(&chainID, &seq); err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE ordered_jobs
			SET status = 'PENDING', updated_at = GETUTCDATE()
			WHERE chain_id = @p1 AND sequence = @p2 AND status = 'BLOCKED'
		`, chainID, seq+1)
		return err
	})
	return err
}

func (r *Repository) PromoteNext(ctx context.Context, chainID string, completedSeq int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE ordered_jobs
		SET status = 'PENDING', updated_at = GETUTCDATE()
		WHERE chain_id = @p1 AND sequence = @p2 AND status = 'BLOCKED'
	`, chainID, completedSeq+1)
	return err
}

func (r *Repository) ListPendingChains(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT chain_id
		FROM ordered_jobs
		WHERE status IN ('PENDING', 'RETRYING')
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var chains []string
	for rows.Next() {
		var ch string
		if err := rows.Scan(&ch); err != nil {
			return nil, err
		}
		chains = append(chains, ch)
	}
	return chains, nil
}

func lockAppChain(ctx context.Context, tx *sql.Tx, chainID string) error {
	_, err := tx.ExecContext(ctx, "sp_getapplock",
		sql.Named("Resource", chainID),
		sql.Named("LockMode", "Exclusive"),
		sql.Named("LockOwner", "Transaction"),
	)
	return err
}

func findExistingByIdempotencyKey(ctx context.Context, tx *sql.Tx, key string) (orderedjob.Job, bool, error) {
	if key == "" {
		return orderedjob.Job{}, false, nil
	}
	query := `
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE idempotency_key = @p1
	`
	row := tx.QueryRowContext(ctx, query, key)
	job, err := scanJob(row)
	if err == nil {
		return job, true, nil
	} else if errors.Is(err, sql.ErrNoRows) {
		return orderedjob.Job{}, false, nil
	}
	return orderedjob.Job{}, false, err
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(s scannable) (orderedjob.Job, error) {
	var j orderedjob.Job
	var idStr string
	var payloadStr string
	var workerID, idempotencyKey, tenantID, lastError, traceID sql.NullString
	var startedAt, completedAt, failedAt sql.NullTime

	err := s.Scan(
		&idStr, &j.ChainID, &j.Sequence, &j.Type, &payloadStr, &j.Status, &j.Attempt, &j.MaxAttempts,
		&j.AvailableAt, &j.DeadlineAt, &workerID, &j.LeaseUntil, &j.LeaseGeneration,
		&j.CreatedAt, &j.UpdatedAt, &startedAt, &completedAt, &failedAt,
		&idempotencyKey, &tenantID, &lastError, &traceID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
		return orderedjob.Job{}, err
	}

	j.ID, _ = uuid.Parse(idStr)
	j.Payload = json.RawMessage(payloadStr)
	if workerID.Valid {
		j.WorkerID = workerID.String
	}
	if idempotencyKey.Valid {
		j.IdempotencyKey = idempotencyKey.String
	}
	if tenantID.Valid {
		j.TenantID = tenantID.String
	}
	if lastError.Valid {
		j.LastError = lastError.String
	}
	if traceID.Valid {
		j.TraceID = traceID.String
	}
	if startedAt.Valid {
		j.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	if failedAt.Valid {
		j.FailedAt = &failedAt.Time
	}

	return j, nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func distinctChains(reqs []orderedjob.EnqueueRequest) []string {
	set := map[string]struct{}{}
	for _, r := range reqs {
		if r.ChainID != "" {
			set[r.ChainID] = struct{}{}
		}
	}
	res := make([]string, 0, len(set))
	for k := range set {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *Repository) GetStats(ctx context.Context) (orderedjob.Stats, error) {
	var stats orderedjob.Stats
	rows, err := r.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM ordered_jobs
		GROUP BY status
	`)
	if err != nil {
		return stats, err
	}
	defer func() { _ = rows.Close() }()

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

	err = r.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT chain_id) FROM ordered_jobs`).Scan(&stats.ActiveChains)
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
		whereClauses = append(whereClauses, fmt.Sprintf("chain_id = @p%d", argIdx))
		args = append(args, filter.ChainID)
		argIdx++
	}
	if filter.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = @p%d", argIdx))
		args = append(args, filter.Status)
		argIdx++
	}
	if filter.JobType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("job_type = @p%d", argIdx))
		args = append(args, filter.JobType)
		argIdx++
	}
	if filter.Search != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(CAST(id AS VARCHAR(36)) LIKE @p%d OR idempotency_key LIKE @p%d OR trace_id LIKE @p%d)", argIdx, argIdx, argIdx))
		args = append(args, "%"+filter.Search+"%")
		argIdx++
	}

	whereStmt := strings.Join(whereClauses, " AND ")

	// #nosec G201
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM ordered_jobs WHERE %s", whereStmt)
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
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

	// #nosec G201
	dataQuery := fmt.Sprintf(`
		SELECT id, chain_id, sequence, job_type, payload, status, attempt, max_attempts, available_at, deadline_at, COALESCE(worker_id, ''), lease_until, lease_generation, created_at, updated_at, started_at, completed_at, failed_at, COALESCE(idempotency_key, ''), COALESCE(tenant_id, ''), COALESCE(last_error, ''), COALESCE(trace_id, '')
		FROM ordered_jobs
		WHERE %s
		ORDER BY %s %s
		OFFSET @p%d ROWS FETCH NEXT @p%d ROWS ONLY
	`, whereStmt, orderByCol, orderDir, argIdx, argIdx+1)

	args = append(args, offset, limit)

	rows, err := r.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

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
		whereClause = fmt.Sprintf("WHERE chain_id LIKE @p%d", argIdx)
		args = append(args, "%"+filter.Search+"%")
		argIdx++
	}

	// #nosec G201
	countQuery := fmt.Sprintf("SELECT COUNT(DISTINCT chain_id) FROM ordered_jobs %s", whereClause)
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
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

	// #nosec G201
	dataQuery := fmt.Sprintf(`
		SELECT 
			chain_id,
			COUNT(*) as total_jobs,
			MAX(sequence) as max_sequence,
			SUM(CASE WHEN status IN ('PENDING', 'PROCESSING', 'RETRYING') THEN 1 ELSE 0 END) as pending_jobs,
			SUM(CASE WHEN status IN ('FAILED', 'DEAD_LETTERED') THEN 1 ELSE 0 END) as failed_jobs
		FROM ordered_jobs
		%s
		GROUP BY chain_id
		ORDER BY %s %s
		OFFSET @p%d ROWS FETCH NEXT @p%d ROWS ONLY
	`, whereClause, orderByCol, orderDir, argIdx, argIdx+1)

	args = append(args, offset, limit)

	rows, err := r.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	var summaries []orderedjob.ChainSummary
	for rows.Next() {
		var cs orderedjob.ChainSummary
		if err := rows.Scan(&cs.ChainID, &cs.TotalJobs, &cs.MaxSequence, &cs.PendingJobs, &cs.FailedJobs); err != nil {
			return nil, 0, err
		}

		_ = r.db.QueryRowContext(ctx, "SELECT status FROM ordered_jobs WHERE chain_id = @p1 AND sequence = @p2", cs.ChainID, cs.MaxSequence).Scan(&cs.LatestStatus)

		summaries = append(summaries, cs)
	}

	return summaries, total, nil
}
