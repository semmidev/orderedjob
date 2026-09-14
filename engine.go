package orderedjob

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/destel/rill"
	"github.com/google/uuid"
	"github.com/semmidev/orderedjob/lease"
	"github.com/semmidev/orderedjob/retry"
)

// PanicHandler handles panics caught during job execution.
type PanicHandler func(ctx context.Context, job Job, panicVal any, stack []byte) error

// Engine is the core orchestrator.
type Engine struct {
	repo     Repository
	handlers *Registry
	leaseMgr lease.Manager
	retryPol retry.Policy
	metrics  Metrics
	logger   Logger

	workerID         string
	concurrency      int
	pollInterval     time.Duration
	recoveryInterval time.Duration
	useNotify        bool
	allowGap         bool
	orderingMode     string // strict, skip-on-failure, dead-letter-and-continue
	orderingStrategy OrderingStrategyResolver
	panicHandler     PanicHandler
	retryOnPanic     bool

	// lifecycle
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	notifyCh chan string // for LISTEN/NOTIFY wakeup
}

// OrderingStrategyResolver resolves ordering mode per job request dynamically.
type OrderingStrategyResolver func(req EnqueueRequest) string

type Option func(*Engine)

func WithConcurrency(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.concurrency = n
		}
	}
}
func WithPollInterval(d time.Duration) Option {
	return func(e *Engine) { e.pollInterval = d }
}
func WithLease(d time.Duration) Option {
	return func(e *Engine) { e.leaseMgr = lease.New(d) }
}
func WithRetryPolicy(p retry.Policy) Option {
	return func(e *Engine) { e.retryPol = p }
}
func WithMetrics(m Metrics) Option {
	return func(e *Engine) { e.metrics = m }
}
func WithLogger(l Logger) Option {
	return func(e *Engine) { e.logger = l }
}
func WithSlogLogger(l *slog.Logger) Option {
	return func(e *Engine) { e.logger = NewSlogLogger(l) }
}
func WithWorkerID(id string) Option {
	return func(e *Engine) { e.workerID = id }
}
func WithRecoveryInterval(d time.Duration) Option {
	return func(e *Engine) { e.recoveryInterval = d }
}
func WithNotify(enabled bool) Option {
	return func(e *Engine) { e.useNotify = enabled }
}
func WithOrderingMode(mode string) Option {
	return func(e *Engine) { e.orderingMode = mode }
}
func WithOrderingStrategy(s OrderingStrategyResolver) Option {
	return func(e *Engine) { e.orderingStrategy = s }
}
func WithPanicHandler(h PanicHandler) Option {
	return func(e *Engine) { e.panicHandler = h }
}
func WithRetryOnPanic(retry bool) Option {
	return func(e *Engine) { e.retryOnPanic = retry }
}

func New(repo Repository, opts ...Option) *Engine {
	e := &Engine{
		repo:             repo,
		handlers:         NewRegistry(),
		leaseMgr:         lease.New(60 * time.Second),
		retryPol:         retry.DefaultPolicy(),
		metrics:          NoopMetrics{},
		logger:           NoopLogger{},
		workerID:         fmt.Sprintf("worker-%s", uuid.New().String()[:8]),
		concurrency:      10,
		pollInterval:     500 * time.Millisecond,
		recoveryInterval: 10 * time.Second,
		orderingMode:     OrderingModeStrict,
		notifyCh:         make(chan string, 100),
	}
	for _, o := range opts {
		o(e)
	}
	if e.retryPol.MaxAttempts == 0 {
		e.retryPol = retry.DefaultPolicy()
	}
	return e
}

// Register handler.
func (e *Engine) Register(jobType string, h Handler) {
	e.handlers.Register(jobType, h)
}
func (e *Engine) RegisterFunc(jobType string, fn func(ctx context.Context, job Job) error) {
	e.handlers.Register(jobType, HandlerFunc(fn))
}

// JobTypes returns all registered job type names in sorted order.
func (e *Engine) JobTypes() []string {
	return e.handlers.JobTypes()
}

// Enqueue delegates to repo.
func (e *Engine) Enqueue(ctx context.Context, req EnqueueRequest) (Job, error) {
	if req.Type == "" {
		return Job{}, fmt.Errorf("job type required")
	}
	if req.ChainID == "" {
		return Job{}, fmt.Errorf("chain_id required")
	}
	if req.OrderingMode == "" {
		if e.orderingStrategy != nil {
			req.OrderingMode = e.orderingStrategy(req)
		}
		if req.OrderingMode == "" {
			req.OrderingMode = e.orderingMode
		}
	}
	if req.OrderingMode == "" {
		req.OrderingMode = OrderingModeStrict
	}
	if req.TraceID == "" {
		req.TraceID = ExtractTraceID(ctx)
	}
	return e.repo.Enqueue(ctx, req)
}
func (e *Engine) EnqueueBatch(ctx context.Context, reqs []EnqueueRequest) ([]Job, error) {
	for i := range reqs {
		if reqs[i].OrderingMode == "" {
			if e.orderingStrategy != nil {
				reqs[i].OrderingMode = e.orderingStrategy(reqs[i])
			}
			if reqs[i].OrderingMode == "" {
				reqs[i].OrderingMode = e.orderingMode
			}
		}
		if reqs[i].OrderingMode == "" {
			reqs[i].OrderingMode = OrderingModeStrict
		}
	}
	return e.repo.EnqueueBatch(ctx, reqs)
}
func (e *Engine) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	return e.repo.Get(ctx, id)
}
func (e *Engine) ReplayJob(ctx context.Context, id uuid.UUID) error {
	return e.repo.ReplayJob(ctx, id)
}

func (e *Engine) SkipJob(ctx context.Context, id uuid.UUID) error {
	return e.repo.SkipJob(ctx, id)
}

// Schedule enqueues a job scheduled for execution at runAt.
func (e *Engine) Schedule(ctx context.Context, req EnqueueRequest, runAt time.Time) (Job, error) {
	req.AvailableAt = &runAt
	return e.Enqueue(ctx, req)
}

// EnqueueDelayed enqueues a job scheduled for execution after delay.
func (e *Engine) EnqueueDelayed(ctx context.Context, req EnqueueRequest, delay time.Duration) (Job, error) {
	runAt := time.Now().Add(delay)
	req.AvailableAt = &runAt
	return e.Enqueue(ctx, req)
}

// RescheduleJob modifies the scheduled available_at timestamp of a job.
func (e *Engine) RescheduleJob(ctx context.Context, id uuid.UUID, newAvailableAt time.Time) (Job, error) {
	return e.repo.RescheduleJob(ctx, id, newAvailableAt)
}

func (e *Engine) listenNotifyLoop() {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error(e.ctx, "listenNotifyLoop recovered from panic", "panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
		}
		e.wg.Done()
	}()
	err := e.repo.Listen(e.ctx, func(chainID string) {
		e.NotifyChain(chainID)
	})
	if err != nil && e.ctx.Err() == nil {
		e.logger.Error(e.ctx, "listen notify error", "error", err.Error())
	}
}

// Start launches workers and recovery loop.
func (e *Engine) Start(ctx context.Context) error {
	if e.ctx != nil {
		return fmt.Errorf("engine already started")
	}
	e.ctx, e.cancel = context.WithCancel(ctx)

	for i := 0; i < e.concurrency; i++ {
		e.wg.Add(1)
		go e.workerLoop(i)
	}

	// recovery worker
	e.wg.Add(1)
	go e.recoveryLoop()

	// optional notify listener
	if e.useNotify && e.repo.NotifyChannel() != "" {
		e.wg.Add(1)
		go e.listenNotifyLoop()
	}

	e.logger.Info(e.ctx, "engine started", "worker_id", e.workerID, "concurrency", e.concurrency, "lease", e.leaseMgr.LeaseDuration.String())
	return nil
}

func (e *Engine) Shutdown(ctx context.Context) error {
	if e.cancel == nil {
		return nil
	}
	e.cancel()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		e.logger.Info(context.Background(), "engine shutdown gracefully")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workerLoop polls and executes.
func (e *Engine) workerLoop(idx int) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error(e.ctx, "workerLoop recovered from panic", "worker_idx", idx, "panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
		}
		e.wg.Done()
	}()
	workerName := fmt.Sprintf("%s-%d", e.workerID, idx)

	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// #nosec G404 - pseudo-random jitter for polling interval
			ticker.Reset(e.pollInterval + time.Duration(rand.Int63n(int64(e.pollInterval/2))))
			e.tryClaimAndExecute(workerName)
		case chainID := <-e.notifyCh:
			e.logger.Debug(e.ctx, "notify wakeup", "chain_id", chainID)
			e.tryClaimAndExecute(workerName)
		}
	}
}

func (e *Engine) tryClaimAndExecute(workerName string) {
	job, err := e.repo.Claim(e.ctx, workerName, e.leaseMgr.LeaseDuration)
	if err != nil {
		if err != ErrNotFound {
			e.logger.Debug(e.ctx, "claim error", "error", err.Error())
		}
		return
	}

	e.metrics.IncClaimed()
	queueDelay := time.Since(job.CreatedAt)
	e.metrics.ObserveQueueDelay(job.Type, queueDelay)

	hctx, hcancel := context.WithCancel(e.ctx)
	defer hcancel()

	go e.heartbeatLoop(hctx, job)

	start := time.Now()
	h, err := e.handlers.MustGet(job.Type)
	if err != nil {
		_ = e.repo.Fail(e.ctx, job.ID, workerName, job.LeaseGeneration, fmt.Sprintf("handler not found: %s", job.Type), true)
		e.metrics.IncFailed(job.Type)
		hcancel()
		return
	}

	execCtx := WithTraceID(hctx, job.TraceID)
	if job.DeadlineAt != nil {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(execCtx, *job.DeadlineAt)
		defer cancel()
	}

	var execErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				dur := time.Since(start)
				e.metrics.ObserveExecDuration(job.Type, dur)
				stack := debug.Stack()
				e.metrics.IncPanics(job.Type)

				e.logger.Error(execCtx, "job handler panicked", append(LogJobAttrs(job), "panic", fmt.Sprintf("%v", r), "stack", string(stack))...)

				if e.panicHandler != nil {
					execErr = e.panicHandler(execCtx, job, r, stack)
				} else {
					pErr := &PanicError{Value: r, Stack: stack}
					if e.retryOnPanic {
						execErr = Retryable(pErr)
					} else {
						execErr = NonRetryable(pErr)
					}
				}
			}
		}()
		execErr = h.Handle(execCtx, job)
		dur := time.Since(start)
		e.metrics.ObserveExecDuration(job.Type, dur)
	}()

	hcancel()

	if execErr == nil {
		if err := e.repo.Complete(e.ctx, job.ID, workerName, job.LeaseGeneration); err != nil {
			if err == ErrLeaseConflict {
				e.logger.Error(e.ctx, "fencing violation on complete", append(LogJobAttrs(job), "error", err.Error())...)
				e.metrics.IncClaimConflicts()
			} else {
				e.logger.Error(e.ctx, "complete failed", append(LogJobAttrs(job), "error", err.Error())...)
			}
			return
		}
		e.metrics.IncCompleted(job.Type)
		e.logger.Info(e.ctx, "job completed", LogJobAttrs(job)...)
		select {
		case e.notifyCh <- job.ChainID:
		default:
		}
		return
	}

	if errorsIsCancel(execErr) {
		_ = e.repo.RequestCancel(e.ctx, job.ID)
	}

	if IsNonRetryable(execErr) || job.Attempt >= job.MaxAttempts || !e.retryPol.ShouldRetry(job.Attempt) {
		mode := job.OrderingMode
		if mode == "" {
			mode = e.orderingMode
		}
		if mode == "" {
			mode = OrderingModeStrict
		}

		_ = e.repo.Fail(e.ctx, job.ID, workerName, job.LeaseGeneration, execErr.Error(), true)
		e.metrics.IncFailed(job.Type)
		e.logger.Error(e.ctx, "job failed terminally", append(LogJobAttrs(job), "error", execErr.Error(), "ordering_mode", mode)...)

		if mode == OrderingModeSkipOnFailure || mode == OrderingModeDeadLetterAndContinue {
			_ = e.repo.PromoteNext(e.ctx, job.ChainID, job.Sequence)
			select {
			case e.notifyCh <- job.ChainID:
			default:
			}
		}
		return
	}
	next := time.Now().UTC().Add(e.retryPol.Backoff(job.Attempt))
	_ = e.repo.Retry(e.ctx, job.ID, workerName, job.LeaseGeneration, execErr.Error(), next)
	e.metrics.IncRetry(job.Type)
	e.logger.Info(e.ctx, "job retrying", append(LogJobAttrs(job), "next_available", next, "error", execErr.Error())...)
}

func (e *Engine) heartbeatLoop(ctx context.Context, job Job) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error(ctx, "heartbeatLoop recovered from panic", "job_id", job.ID.String(), "panic", fmt.Sprintf("%v", r))
		}
	}()
	ticker := time.NewTicker(e.leaseMgr.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := e.repo.Heartbeat(ctx, job.ID, job.WorkerID, job.LeaseGeneration, e.leaseMgr.LeaseDuration)
			if err != nil {
				e.logger.Debug(ctx, "heartbeat failed", "job_id", job.ID.String(), "error", err.Error())
				return
			}
		}
	}
}

func (e *Engine) recoveryLoop() {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error(e.ctx, "recoveryLoop recovered from panic", "panic", fmt.Sprintf("%v", r))
		}
		e.wg.Done()
	}()
	ticker := time.NewTicker(e.recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			recovered, err := e.repo.RecoverStale(e.ctx, 100, func(attempt int) time.Duration {
				return e.retryPol.Backoff(attempt)
			})
			if err != nil {
				e.logger.Error(e.ctx, "recovery error", "error", err.Error())
				continue
			}
			if recovered > 0 {
				e.metrics.IncStaleRecovered(recovered)
				e.logger.Info(e.ctx, "stale jobs recovered", "count", recovered)
				_ = rill.ForEach(rill.FromSlice([]int{recovered}, nil), e.concurrency, func(_ int) error {
					return nil
				})
				for i := 0; i < recovered; i++ {
					select {
					case e.notifyCh <- "recovered":
					default:
					}
				}
			}
		}
	}
}

func (e *Engine) NotifyChain(chainID string) {
	select {
	case e.notifyCh <- chainID:
	default:
	}
}

func errorsIsCancel(err error) bool {
	if err == nil {
		return false
	}
	for err != nil {
		if err.Error() == "context canceled" || err.Error() == "context deadline exceeded" {
			return true
		}
		if ue, ok := err.(interface{ Unwrap() error }); ok {
			err = ue.Unwrap()
		} else {
			break
		}
	}
	return false
}
