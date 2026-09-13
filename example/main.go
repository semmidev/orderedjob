package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	"github.com/semmidev/orderedjob/retry"
)

type OrderPayload struct {
	Chain string `json:"chain"`
	Step  int    `json:"step"`
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/orderedjob?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	repo := pgRepo.New(pool)
	if err := repo.Migrate(ctx); err != nil {
		panic(err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(10),
		orderedjob.WithPollInterval(200*time.Millisecond),
		orderedjob.WithLease(30*time.Second),
		orderedjob.WithSlogLogger(logger),
		orderedjob.WithRetryPolicy(retry.Policy{
			MaxAttempts: 5,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    30 * time.Second,
			Jitter:      retry.JitterEqual,
		}),
	)

	// Register type-safe handler with automatic payload unmarshaling
	orderedjob.RegisterTyped(eng, "ProcessOrder", func(ctx context.Context, job orderedjob.Job, payload OrderPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[worker] processing chain=%s seq=%d step=%d trace_id=%s\n", job.ChainID, job.Sequence, payload.Step, traceID)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Shutdown(ctx)

	// Attach TraceID to context for distributed tracing propagation
	ctx = orderedjob.WithTraceID(ctx, "trace-abc-123")

	// Enqueue example: 3 chains x 5 sequential jobs
	for chain := 1; chain <= 3; chain++ {
		chainID := fmt.Sprintf("order-%d", chain)
		for seq := 1; seq <= 5; seq++ {
			_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
				ChainID:  chainID,
				Sequence: int64(seq), // explicit; use 0 for auto
				Type:     "ProcessOrder",
				Payload:  OrderPayload{Chain: chainID, Step: seq},
			})
			if err != nil {
				fmt.Println("enqueue error:", err)
			}
		}
	}

	// Auto sequence example
	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID: "order-auto",
		Type:    "ProcessOrder",
		Payload: OrderPayload{Chain: "order-auto", Step: 1},
	})

	fmt.Println("jobs enqueued, waiting 5s...")
	time.Sleep(5 * time.Second)
	fmt.Println("done")
}
