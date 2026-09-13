package postgres_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgContainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestPostgres(t *testing.T, ctx context.Context) (*pgxpool.Pool, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping real PostgreSQL testcontainers integration test in -short mode")
	}

	// Auto-detect Podman / Docker socket environments:
	// If TESTCONTAINERS_RYUK_DISABLED is not explicitly set, check if disabling Ryuk helps Podman compatibility.
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		if isPodmanOrCustomSocket() {
			_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		}
	}

	reqOpts := []testcontainers.ContainerCustomizer{
		pgContainer.WithDatabase("orderedjob_test"),
		pgContainer.WithUsername("postgres"),
		pgContainer.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30 * time.Second),
		),
	}

	container, err := pgContainer.Run(ctx, "postgres:17-alpine", reqOpts...)
	if err != nil && isPodmanOrNetworkError(err) {
		// Fallback retry with Ryuk disabled
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		container, err = pgContainer.Run(ctx, "postgres:17-alpine", reqOpts...)
	}

	if err != nil {
		t.Skipf("skipping testcontainers integration test (Docker/Podman daemon might be unavailable): %v", err)
		return nil, func() {}
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		_ = container.Terminate(ctx)
	}

	return pool, cleanup
}

func isPodmanOrCustomSocket() bool {
	dockerHost := strings.ToLower(os.Getenv("DOCKER_HOST"))
	return strings.Contains(dockerHost, "podman") || os.Getenv("PODMAN_SYSTEM_SOCKET") != ""
}

func isPodmanOrNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "bridge") ||
		strings.Contains(msg, "network not found") ||
		strings.Contains(msg, "reaper") ||
		strings.Contains(msg, "ryuk") ||
		strings.Contains(msg, "podman")
}

func TestPostgresRepository_TableDriven(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestPostgres(t, ctx)
	if pool == nil {
		return
	}
	defer cleanup()

	repo := pgRepo.New(pool)
	err := repo.Migrate(ctx)
	require.NoError(t, err, "database schema migration must succeed")

	t.Run("Enqueue and Claim FIFO Order", func(t *testing.T) {
		chainID := "testcontainers-fifo-chain"

		// Enqueue 3 sequential jobs
		for seq := 1; seq <= 3; seq++ {
			req := orderedjob.EnqueueRequest{
				ChainID:  chainID,
				Sequence: int64(seq),
				Type:     "PaymentJob",
				Payload:  map[string]any{"seq": seq},
			}
			job, err := repo.Enqueue(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, int64(seq), job.Sequence)
		}

		// Claim first job, sequence 1 must be claimed first
		job1, err := repo.Claim(ctx, "worker-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(1), job1.Sequence)

		// Complete job 1
		err = repo.Complete(ctx, job1.ID, job1.WorkerID, job1.LeaseGeneration)
		require.NoError(t, err)

		// Claim next job, sequence 2 must be claimed next
		job2, err := repo.Claim(ctx, "worker-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(2), job2.Sequence)
	})

	t.Run("Batch Enqueue in Single Transaction", func(t *testing.T) {
		chainID := "testcontainers-batch-chain"

		batchReqs := []orderedjob.EnqueueRequest{
			{ChainID: chainID, Sequence: 0, Type: "BatchTask", Payload: map[string]int{"step": 1}},
			{ChainID: chainID, Sequence: 0, Type: "BatchTask", Payload: map[string]int{"step": 2}},
		}

		jobs, err := repo.EnqueueBatch(ctx, batchReqs)
		require.NoError(t, err)
		require.Len(t, jobs, 2)

		assert.Equal(t, int64(1), jobs[0].Sequence)
		assert.Equal(t, int64(2), jobs[1].Sequence)
	})

	t.Run("Idempotency Key Deduplication", func(t *testing.T) {
		chainID := "testcontainers-idemp-chain"
		idempKey := "unique-evt-id-998877"

		req := orderedjob.EnqueueRequest{
			ChainID:        chainID,
			Sequence:       1,
			Type:           "AuditJob",
			IdempotencyKey: idempKey,
		}

		job1, err1 := repo.Enqueue(ctx, req)
		require.NoError(t, err1)

		// Duplicate enqueue with same idempotency key
		job2, err2 := repo.Enqueue(ctx, req)
		require.NoError(t, err2)

		assert.Equal(t, job1.ID, job2.ID, "idempotent enqueue must return existing job")
	})
}
