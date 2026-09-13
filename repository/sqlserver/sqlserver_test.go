package sqlserver_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/semmidev/orderedjob"
	mssqlRepo "github.com/semmidev/orderedjob/repository/sqlserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	mssqlContainer "github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestMSSQL(t *testing.T, ctx context.Context) (*sql.DB, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping real SQL Server testcontainers integration test in -short mode")
	}

	container, err := mssqlContainer.Run(ctx,
		"mcr.microsoft.com/mssql/server:2022-latest",
		mssqlContainer.WithAcceptEULA(),
		mssqlContainer.WithPassword("StrongPassword123!"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("SQL Server is now ready for client connections").
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Skipf("skipping testcontainers mssql integration test (Docker daemon might be unavailable): %v", err)
		return nil, func() {}
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Skipf("skipping mssql connection string error: %v", err)
		_ = container.Terminate(ctx)
		return nil, func() {}
	}

	db, err := sql.Open("mssql", connStr)
	require.NoError(t, err)

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	return db, cleanup
}

func TestSQLServerRepository_TableDriven(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestMSSQL(t, ctx)
	if db == nil {
		return
	}
	defer cleanup()

	repo := mssqlRepo.New(db)
	err := repo.Migrate(ctx)
	require.NoError(t, err, "database schema migration must succeed")

	t.Run("Enqueue and Claim FIFO Order", func(t *testing.T) {
		chainID := "mssql-fifo-chain"

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
		job1, err := repo.Claim(ctx, "worker-mssql-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(1), job1.Sequence)

		// Complete job 1
		err = repo.Complete(ctx, job1.ID, job1.WorkerID, job1.LeaseGeneration)
		require.NoError(t, err)

		// Claim next job, sequence 2 must be claimed next
		job2, err := repo.Claim(ctx, "worker-mssql-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(2), job2.Sequence)
	})

	t.Run("Batch Enqueue in Single Transaction", func(t *testing.T) {
		chainID := "mssql-batch-chain"

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
		chainID := "mssql-idemp-chain"
		idempKey := "mssql-unique-evt-id-998877"

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
