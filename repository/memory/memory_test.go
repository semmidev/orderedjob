package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/semmidev/orderedjob"
	memoryRepo "github.com/semmidev/orderedjob/repository/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryRepository_TableDriven(t *testing.T) {
	ctx := context.Background()

	t.Run("Enqueue and Claim Operations", func(t *testing.T) {
		repo := memoryRepo.New()

		tests := []struct {
			name        string
			req         orderedjob.EnqueueRequest
			expectedSeq int64
			expectErr   bool
		}{
			{
				name:        "auto sequence first job",
				req:         orderedjob.EnqueueRequest{ChainID: "chain-mem", Sequence: 0, Type: "TaskA"},
				expectedSeq: 1,
				expectErr:   false,
			},
			{
				name:        "auto sequence second job",
				req:         orderedjob.EnqueueRequest{ChainID: "chain-mem", Sequence: 0, Type: "TaskB"},
				expectedSeq: 2,
				expectErr:   false,
			},
			{
				name:        "explicit sequence gap error",
				req:         orderedjob.EnqueueRequest{ChainID: "chain-gap", Sequence: 10, Type: "TaskC"},
				expectedSeq: 0,
				expectErr:   true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				job, err := repo.Enqueue(ctx, tt.req)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expectedSeq, job.Sequence)
				}
			})
		}

		// Claim first job
		job1, err := repo.Claim(ctx, "worker-mem-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(1), job1.Sequence)

		// Complete job 1
		err = repo.Complete(ctx, job1.ID, job1.WorkerID, job1.LeaseGeneration)
		require.NoError(t, err)

		// Claim next job, sequence 2 must be claimed next
		job2, err := repo.Claim(ctx, "worker-mem-1", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int64(2), job2.Sequence)
	})

	t.Run("Batch Enqueue Operations", func(t *testing.T) {
		repo := memoryRepo.New()

		batchReqs := []orderedjob.EnqueueRequest{
			{ChainID: "chain-batch", Sequence: 0, Type: "BatchA"},
			{ChainID: "chain-batch", Sequence: 0, Type: "BatchB"},
		}

		jobs, err := repo.EnqueueBatch(ctx, batchReqs)
		require.NoError(t, err)
		require.Len(t, jobs, 2)
		assert.Equal(t, int64(1), jobs[0].Sequence)
		assert.Equal(t, int64(2), jobs[1].Sequence)
	})
}
