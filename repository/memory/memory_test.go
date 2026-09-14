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

	t.Run("Reschedule Operations and Scheduled Filters", func(t *testing.T) {
		repo := memoryRepo.New()
		future := time.Now().Add(1 * time.Hour)
		req := orderedjob.EnqueueRequest{ChainID: "chain-resched", Sequence: 1, Type: "TaskScheduled", AvailableAt: &future}

		j, err := repo.Enqueue(ctx, req)
		require.NoError(t, err)

		stats, err := repo.GetStats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(1), stats.Scheduled)

		newTime := time.Now().Add(10 * time.Minute)
		updated, err := repo.RescheduleJob(ctx, j.ID, newTime)
		require.NoError(t, err)
		assert.WithinDuration(t, newTime, updated.AvailableAt, 1*time.Second)

		// Reschedule job that does not exist
		_, err = repo.RescheduleJob(ctx, [16]byte{}, newTime)
		assert.ErrorIs(t, err, orderedjob.ErrNotFound)
	})

	t.Run("ReplayJobWithPayload and Tenant/Trace Filtering", func(t *testing.T) {
		repo := memoryRepo.New()
		j, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-tenant",
			Sequence: 1,
			Type:     "TenantTask",
			TenantID: "tenant-99",
			TraceID:  "trace-abc-123",
			Payload:  map[string]string{"foo": "bar"},
		})
		require.NoError(t, err)

		// Claim job first so workerID and leaseGeneration match
		claimed, err := repo.Claim(ctx, "w1", 10*time.Second)
		require.NoError(t, err)

		// Fail job so it can be replayed
		err = repo.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "failed test", true)
		require.NoError(t, err)

		// Replay with new payload
		newPayload := []byte(`{"foo":"replayed"}`)
		err = repo.ReplayJobWithPayload(ctx, j.ID, newPayload)
		require.NoError(t, err)

		updated, err := repo.Get(ctx, j.ID)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", updated.Status)
		assert.JSONEq(t, string(newPayload), string(updated.Payload))

		// Filter by TenantID
		jobsTenant, totalTenant, err := repo.ListJobs(ctx, orderedjob.JobFilter{TenantID: "tenant-99"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), totalTenant)
		assert.Len(t, jobsTenant, 1)
		assert.Equal(t, j.ID, jobsTenant[0].ID)

		// Filter by wrong TenantID
		jobsWrongTenant, totalWrong, err := repo.ListJobs(ctx, orderedjob.JobFilter{TenantID: "tenant-other"})
		require.NoError(t, err)
		assert.Equal(t, int64(0), totalWrong)
		assert.Empty(t, jobsWrongTenant)

		// Filter by TraceID
		jobsTrace, totalTrace, err := repo.ListJobs(ctx, orderedjob.JobFilter{TraceID: "trace-abc-123"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), totalTrace)
		assert.Len(t, jobsTrace, 1)
		assert.Equal(t, j.ID, jobsTrace[0].ID)
	})
}
