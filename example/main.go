package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	"github.com/semmidev/orderedjob/retry"
)

// OrderPayload represents domain data for an order processing pipeline.
type OrderPayload struct {
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
	Step      int     `json:"step"`
}

// NotificationPayload represents domain data for email/SMS notifications.
type NotificationPayload struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
}

// CustomMetrics demonstrates implementing orderedjob.Metrics following Prometheus & OpenTelemetry conventions.
type CustomMetrics struct {
	Claimed       atomic.Int64
	Completed     atomic.Int64
	Failed        atomic.Int64
	Retries       atomic.Int64
	Recovered     atomic.Int64
	Conflicts     atomic.Int64
	ActiveLeases  atomic.Int64
	BlockedChains atomic.Int64
	TotalExecNs   atomic.Int64
	ExecCount     atomic.Int64
}

// Prometheus counterpart: orderedjob_jobs_claimed_total (Counter)
func (m *CustomMetrics) IncClaimed() { m.Claimed.Add(1) }

// Prometheus counterpart: orderedjob_jobs_completed_total{job_type="..."} (Counter)
func (m *CustomMetrics) IncCompleted(jobType string) { m.Completed.Add(1) }

// Prometheus counterpart: orderedjob_jobs_failed_total{job_type="..."} (Counter)
func (m *CustomMetrics) IncFailed(jobType string) { m.Failed.Add(1) }

// Prometheus counterpart: orderedjob_job_retries_total{job_type="..."} (Counter)
func (m *CustomMetrics) IncRetry(jobType string) { m.Retries.Add(1) }

// Prometheus counterpart: orderedjob_job_execution_duration_seconds{job_type="..."} (Histogram)
func (m *CustomMetrics) ObserveExecDuration(jobType string, d time.Duration) {
	m.TotalExecNs.Add(d.Nanoseconds())
	m.ExecCount.Add(1)
}

// Prometheus counterpart: orderedjob_job_queue_delay_seconds{job_type="..."} (Histogram)
func (m *CustomMetrics) ObserveQueueDelay(jobType string, d time.Duration) {
	// Map to prometheus.HistogramVec observer in production
}

// Prometheus counterpart: orderedjob_active_leases (Gauge)
func (m *CustomMetrics) SetActiveLeases(n int) { m.ActiveLeases.Store(int64(n)) }

// Prometheus counterpart: orderedjob_claim_conflicts_total (Counter)
func (m *CustomMetrics) IncClaimConflicts() { m.Conflicts.Add(1) }

// Prometheus counterpart: orderedjob_stale_recovered_total (Counter)
func (m *CustomMetrics) IncStaleRecovered(n int) { m.Recovered.Add(int64(n)) }

// Prometheus counterpart: orderedjob_blocked_chains_total (Counter)
func (m *CustomMetrics) IncBlockedChain() { m.BlockedChains.Add(1) }

func (m *CustomMetrics) PrintSummary() {
	fmt.Println("\n📊 --- Metrics Summary (Prometheus / OpenTelemetry Standard) ---")
	fmt.Printf("Claimed Jobs     (orderedjob_jobs_claimed_total)         : %d\n", m.Claimed.Load())
	fmt.Printf("Completed Jobs   (orderedjob_jobs_completed_total)       : %d\n", m.Completed.Load())
	fmt.Printf("Failed Jobs      (orderedjob_jobs_failed_total)          : %d\n", m.Failed.Load())
	fmt.Printf("Retried Jobs     (orderedjob_job_retries_total)          : %d\n", m.Retries.Load())
	fmt.Printf("Recovered Jobs   (orderedjob_stale_recovered_total)      : %d\n", m.Recovered.Load())
	fmt.Printf("Claim Conflicts  (orderedjob_claim_conflicts_total)      : %d\n", m.Conflicts.Load())
	fmt.Printf("Blocked Chains   (orderedjob_blocked_chains_total)       : %d\n", m.BlockedChains.Load())
	fmt.Printf("Active Leases    (orderedjob_active_leases)              : %d\n", m.ActiveLeases.Load())
	
	count := m.ExecCount.Load()
	if count > 0 {
		avgMs := float64(m.TotalExecNs.Load()) / float64(count) / 1e6
		fmt.Printf("Avg Exec Duration(orderedjob_job_execution_duration_sec): %.2f ms (across %d executions)\n", avgMs, count)
	}
	fmt.Println("----------------------------------------------------------------")
}

func main() {
	ctx := context.Background()

	// 1. Database Connection & PGX Pool Setup
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/orderedjob?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to database: %v", err))
	}
	defer pool.Close()

	// 2. Repository & Database Schema Migration
	repo := pgRepo.New(pool)
	if err := repo.Migrate(ctx); err != nil {
		panic(fmt.Sprintf("failed to run database migrations: %v", err))
	}

	// 3. Structured Logging & Metrics Collectors
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	metrics := &CustomMetrics{}

	// 4. Initialize Core Engine with Advanced Options
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(5),
		orderedjob.WithPollInterval(100*time.Millisecond),
		orderedjob.WithLease(15*time.Second),
		orderedjob.WithSlogLogger(logger),
		orderedjob.WithMetrics(metrics),
		orderedjob.WithNotify(true),
		orderedjob.WithRetryPolicy(retry.Policy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    1 * time.Second,
			Jitter:      retry.JitterEqual,
		}),
	)

	// 5. Register Typed Handlers (Go Generics)
	orderedjob.RegisterTyped(eng, "ProcessPayment", func(ctx context.Context, job orderedjob.Job, payload OrderPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[worker] 💳 Processing payment chain=%s seq=%d step=%d trace_id=%s account=%s amount=%.2f tenant=%s\n",
			job.ChainID, job.Sequence, payload.Step, traceID, payload.AccountID, payload.Amount, job.TenantID)
		time.Sleep(30 * time.Millisecond)
		return nil
	})

	orderedjob.RegisterTyped(eng, "SendNotification", func(ctx context.Context, job orderedjob.Job, payload NotificationPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[worker] 📧 Sending notification chain=%s recipient=%s message=%s trace_id=%s\n",
			job.ChainID, payload.Recipient, payload.Message, traceID)
		return nil
	})

	// Demonstrate Transient Error with Automatic Retry
	var failAttempts atomic.Int32
	orderedjob.RegisterTyped(eng, "ValidateAccount", func(ctx context.Context, job orderedjob.Job, payload OrderPayload) error {
		count := failAttempts.Add(1)
		if count == 1 {
			fmt.Printf("[worker] ⚠️  Transient validation failure on chain=%s seq=%d (attempting retry...)\n",
				job.ChainID, job.Sequence)
			return orderedjob.Retryable(fmt.Errorf("temporary network timeout during account validation"))
		}
		fmt.Printf("[worker] ✅ Account validated successfully on chain=%s seq=%d\n", job.ChainID, job.Sequence)
		return nil
	})

	// 6. Register Raw/Dynamic Handler (Untyped JSON)
	eng.RegisterFunc("AuditLog", func(ctx context.Context, job orderedjob.Job) error {
		var raw map[string]any
		_ = json.Unmarshal(job.Payload, &raw)
		fmt.Printf("[worker] 📝 Audit log captured for chain=%s data=%v\n", job.ChainID, raw)
		return nil
	})

	// 7. Start Worker Engine & Stale Lock Recovery Loop
	if err := eng.Start(ctx); err != nil {
		panic(fmt.Sprintf("failed to start orderedjob engine: %v", err))
	}

	// 8. Attach Distributed Tracing ID to Context
	ctx = orderedjob.WithTraceID(ctx, "trace-req-998877")

	// 9. Feature Demo 1: Explicit Sequential FIFO Chains (Strict Order Guarantee)
	fmt.Println("🚀 Enqueuing strict FIFO sequential jobs...")
	for chain := 1; chain <= 2; chain++ {
		chainID := fmt.Sprintf("order-chain-%d", chain)
		for seq := 1; seq <= 3; seq++ {
			_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
				ChainID:     chainID,
				Sequence:    int64(seq), // Explicit sequence index
				Type:        "ProcessPayment",
				Payload:     OrderPayload{AccountID: chainID, Amount: float64(seq * 100), Step: seq},
				TenantID:    "tenant-enterprise",
				MaxAttempts: 3,
			})
			if err != nil {
				fmt.Printf("enqueue error chain=%s seq=%d: %v\n", chainID, seq, err)
			}
		}
	}

	// 10. Feature Demo 2: Single-Transaction Batch Enqueue with Auto-Sequencing
	fmt.Println("🚀 Batch enqueuing jobs in a single DB transaction...")
	batchReqs := []orderedjob.EnqueueRequest{
		{
			ChainID:  "order-batch-chain",
			Sequence: 0, // Auto-assign sequence 1
			Type:     "ValidateAccount",
			Payload:  OrderPayload{AccountID: "acc-batch-101", Step: 1},
			TenantID: "tenant-retail",
		},
		{
			ChainID:  "order-batch-chain",
			Sequence: 0, // Auto-assign sequence 2
			Type:     "ProcessPayment",
			Payload:  OrderPayload{AccountID: "acc-batch-101", Amount: 250.50, Step: 2},
			TenantID: "tenant-retail",
		},
		{
			ChainID:  "order-batch-chain",
			Sequence: 0, // Auto-assign sequence 3
			Type:     "SendNotification",
			Payload:  NotificationPayload{Recipient: "customer@example.com", Message: "Order processed!"},
			TenantID: "tenant-retail",
		},
	}
	if _, err := eng.EnqueueBatch(ctx, batchReqs); err != nil {
		fmt.Printf("batch enqueue error: %v\n", err)
	}

	// 11. Feature Demo 3: Idempotent Enqueue & Scheduled Delayed Job
	fmt.Println("🚀 Enqueuing delayed job with Idempotency Key...")
	delayedTime := time.Now().Add(500 * time.Millisecond)
	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:        "scheduled-chain",
		Sequence:       1,
		Type:           "AuditLog",
		Payload:        map[string]string{"action": "DAILY_BACKUP", "status": "SCHEDULED"},
		IdempotencyKey: "idemp-daily-backup-2026-09-13",
		TenantID:       "tenant-system",
		AvailableAt:    &delayedTime,
	})

	// Wait for worker pool to finish processing pending jobs
	time.Sleep(3 * time.Second)

	// 12. Graceful Engine Shutdown
	fmt.Println("🛑 Shutting down engine...")
	if err := eng.Shutdown(ctx); err != nil {
		fmt.Printf("shutdown error: %v\n", err)
	}

	// 13. Display Metrics Collected During Execution
	metrics.PrintSummary()
	fmt.Println("✅ All feature demonstrations completed successfully!")
}
