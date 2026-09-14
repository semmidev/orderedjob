package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	mssqlRepo "github.com/semmidev/orderedjob/repository/sqlserver"
	"github.com/semmidev/orderedjob/retry"
	"github.com/semmidev/orderedjob/ui"
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

func (m *CustomMetrics) IncClaimed()                 { m.Claimed.Add(1) }
func (m *CustomMetrics) IncCompleted(jobType string) { m.Completed.Add(1) }
func (m *CustomMetrics) IncFailed(jobType string)    { m.Failed.Add(1) }
func (m *CustomMetrics) IncRetry(jobType string)     { m.Retries.Add(1) }
func (m *CustomMetrics) ObserveExecDuration(jobType string, d time.Duration) {
	m.TotalExecNs.Add(d.Nanoseconds())
	m.ExecCount.Add(1)
}
func (m *CustomMetrics) ObserveQueueDelay(jobType string, d time.Duration) {}
func (m *CustomMetrics) SetActiveLeases(n int)                             { m.ActiveLeases.Store(int64(n)) }
func (m *CustomMetrics) IncClaimConflicts()                                { m.Conflicts.Add(1) }
func (m *CustomMetrics) IncStaleRecovered(n int)                           { m.Recovered.Add(int64(n)) }
func (m *CustomMetrics) IncBlockedChain()                                  { m.BlockedChains.Add(1) }

func (m *CustomMetrics) PrintSummary(dbEngine string) {
	fmt.Printf("\n📊 --- Metrics Summary for [%s] (Prometheus Standard) ---\n", dbEngine)
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

	fmt.Println("================================================================")
	fmt.Println("🚀 STARTING ORDEREDJOB POSTGRES ENGINE & WEB UI MONITORING")
	fmt.Println("================================================================")
	runPostgresDemo(ctx)

	// SQL Server demo (disabled as requested):
	// fmt.Println("\n================================================================")
	// fmt.Println("🚀 RUNNING ORDEREDJOB DEMO (PART 2: SQL SERVER ENGINE)")
	// fmt.Println("================================================================")
	// runSQLServerDemo(ctx)
}

func runPostgresDemo(ctx context.Context) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/orderedjob?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Printf("⚠️ Postgres connection failed: %v (make sure docker compose up -d is running)\n", err)
		return
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fmt.Printf("⚠️ Postgres ping failed: %v (skipping Postgres demo)\n", err)
		return
	}

	repo := pgRepo.New(pool)
	if err := repo.Migrate(ctx); err != nil {
		fmt.Printf("⚠️ Postgres migration error: %v\n", err)
		return
	}

	// Start Web UI server sharing the Postgres repository
	handler := ui.NewHandler(repo, ui.WithRootPath("/ui"), ui.WithTitle("OrderedJob Live Monitoring (PostgreSQL)"))
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("⚠️ Web UI server error: %v\n", err)
		}
	}()

	fmt.Println("🌐 Web UI Monitoring Dashboard running at: http://localhost:8080/ui")

	runEngineDemoLongLived(ctx, "PostgreSQL", repo)
}

func runSQLServerDemo(ctx context.Context) {
	dsn := os.Getenv("MSSQL_URL")
	if dsn == "" {
		dsn = "sqlserver://sa:StrongPassword123!@localhost:1433?database=master&encrypt=disable"
	}
	db, err := sql.Open("mssql", dsn)
	if err != nil {
		fmt.Printf("⚠️ SQL Server connection failed: %v (make sure docker compose up -d is running)\n", err)
		return
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("⚠️ SQL Server ping failed: %v (skipping SQL Server demo)\n", err)
		return
	}

	repo := mssqlRepo.New(db)
	if err := repo.Migrate(ctx); err != nil {
		fmt.Printf("⚠️ SQL Server migration error: %v\n", err)
		return
	}

	runEngineDemoLongLived(ctx, "SQL Server", repo)
}

func runEngineDemoLongLived(ctx context.Context, engineName string, repo orderedjob.Repository) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	metrics := &CustomMetrics{}

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

	// Register Type-Safe Handlers
	orderedjob.RegisterTyped(eng, "ProcessPayment", func(ctx context.Context, job orderedjob.Job, payload OrderPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[%s worker] 💳 Processing payment chain=%s seq=%d step=%d trace_id=%s account=%s amount=%.2f tenant=%s\n",
			engineName, job.ChainID, job.Sequence, payload.Step, traceID, payload.AccountID, payload.Amount, job.TenantID)
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	orderedjob.RegisterTyped(eng, "SendNotification", func(ctx context.Context, job orderedjob.Job, payload NotificationPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[%s worker] 📧 Sending notification chain=%s recipient=%s message=%s trace_id=%s\n",
			engineName, job.ChainID, payload.Recipient, payload.Message, traceID)
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	var failAttempts atomic.Int32
	orderedjob.RegisterTyped(eng, "ValidateAccount", func(ctx context.Context, job orderedjob.Job, payload OrderPayload) error {
		count := failAttempts.Add(1)
		if count == 1 {
			fmt.Printf("[%s worker] ⚠️  Transient validation failure on chain=%s seq=%d (attempting retry...)\n",
				engineName, job.ChainID, job.Sequence)
			return orderedjob.Retryable(fmt.Errorf("temporary network timeout during account validation"))
		}
		fmt.Printf("[%s worker] ✅ Account validated successfully on chain=%s seq=%d\n", engineName, job.ChainID, job.Sequence)
		return nil
	})

	eng.RegisterFunc("AuditLog", func(ctx context.Context, job orderedjob.Job) error {
		var raw map[string]any
		_ = json.Unmarshal(job.Payload, &raw)
		fmt.Printf("[%s worker] 📝 Audit log captured for chain=%s data=%v\n", engineName, job.ChainID, raw)
		return nil
	})

	if err := eng.Start(ctx); err != nil {
		panic(fmt.Sprintf("failed to start %s engine: %v", engineName, err))
	}

	ctx = orderedjob.WithTraceID(ctx, fmt.Sprintf("trace-%s-998877", engineName))

	// Enqueue Initial Sequential Seed Jobs
	fmt.Printf("🚀 [%s] Enqueuing initial seed jobs...\n", engineName)
	for chain := 1; chain <= 2; chain++ {
		chainID := fmt.Sprintf("order-chain-%d", chain)
		for seq := 1; seq <= 3; seq++ {
			_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
				ChainID:     chainID,
				Sequence:    int64(seq),
				Type:        "ProcessPayment",
				Payload:     OrderPayload{AccountID: chainID, Amount: float64(seq * 100000), Step: seq},
				TenantID:    "tenant-enterprise",
				MaxAttempts: 3,
			})
		}
	}

	fmt.Printf("⚙️  [%s] Worker engine is active & listening for jobs...\n", engineName)
	fmt.Println("   Open http://localhost:8080/ui in browser to monitor and enqueue jobs.")
	fmt.Println("   Press Ctrl+C to exit.")

	// Keep long-lived process running to handle jobs enqueued via Web UI
	select {}
}
