package orderedjob

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Repository is the persistence port. All coordination decisions use DB time where consistency matters (C16).
type Repository interface {
	// Enqueue inserts a job. If req.Sequence == 0 it auto-assigns.
	Enqueue(ctx context.Context, req EnqueueRequest) (Job, error)
	// EnqueueBatch atomic if required.
	EnqueueBatch(ctx context.Context, reqs []EnqueueRequest) ([]Job, error)

	Get(ctx context.Context, id uuid.UUID) (Job, error)
	// GetByChainSeq fetches by chain+seq
	GetByChainSeq(ctx context.Context, chainID string, seq int64) (Job, error)

	// Claim finds and atomically marks a job as PROCESSING (C3, C4, C10).
	// Returns ErrNotFound if none eligible.
	Claim(ctx context.Context, workerID string, lease time.Duration) (Job, error)

	// Complete marks COMPLETED with fencing check (C10, C5).
	Complete(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error
	// Fail marks FAILED / DEAD_LETTERED
	Fail(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, terminal bool) error
	// Retry marks RETRYING with next available_at (C6)
	Retry(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, nextAvailable time.Time) error

	// Heartbeat extends lease
	Heartbeat(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, lease time.Duration) error

	// Cancel
	RequestCancel(ctx context.Context, id uuid.UUID) error
	ConfirmCancel(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error

	// Recovery & DLQ Operations
	RecoverStale(ctx context.Context, limit int, retryPolicy func(attempt int) time.Duration) (int, error)
	ReplayJob(ctx context.Context, id uuid.UUID) error
	SkipJob(ctx context.Context, id uuid.UUID) error

	// Make next eligible after completion (C5)
	PromoteNext(ctx context.Context, chainID string, completedSeq int64) error

	// List etc
	ListPendingChains(ctx context.Context) ([]string, error)

	// Inspection & Dashboard Methods
	GetStats(ctx context.Context) (Stats, error)
	ListJobs(ctx context.Context, filter JobFilter) ([]Job, int64, error)
	ListChains(ctx context.Context) ([]ChainSummary, error)

	// Real-time LISTEN/NOTIFY interface
	NotifyChannel() string
	Listen(ctx context.Context, callback func(chainID string)) error
}

type JobFilter struct {
	ChainID string
	Status  string
	JobType string
	Search  string
	Offset  int
	Limit   int
}

type Stats struct {
	Pending         int64 `json:"pending"`
	Blocked         int64 `json:"blocked"`
	Processing      int64 `json:"processing"`
	Completed       int64 `json:"completed"`
	Retrying        int64 `json:"retrying"`
	Failed          int64 `json:"failed"`
	DeadLettered    int64 `json:"dead_lettered"`
	CancelRequested int64 `json:"cancel_requested"`
	Cancelled       int64 `json:"cancelled"`
	Total           int64 `json:"total"`
	ActiveChains    int64 `json:"active_chains"`
}

type ChainSummary struct {
	ChainID      string `json:"chain_id"`
	TotalJobs    int64  `json:"total_jobs"`
	MaxSequence  int64  `json:"max_sequence"`
	PendingJobs  int64  `json:"pending_jobs"`
	FailedJobs   int64  `json:"failed_jobs"`
	LatestStatus string `json:"latest_status"`
}
