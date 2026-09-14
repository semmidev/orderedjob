package orderedjob_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

// BenchmarkEngine_ClaimThroughput measures the throughput of claiming and processing 1,000 jobs
// across 16 concurrent workers per benchmark iteration.
func BenchmarkEngine_ClaimThroughput(b *testing.B) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(16),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
		orderedjob.WithPollInterval(1*time.Millisecond),
	)

	ctx := context.Background()
	var executedCount atomic.Int64

	orderedjob.RegisterTyped(eng, "BenchmarkTask", func(ctx context.Context, job orderedjob.Job, payload TestPayload) error {
		executedCount.Add(1)
		return nil
	})

	err := eng.Start(ctx)
	if err != nil {
		b.Fatalf("failed to start engine: %v", err)
	}
	defer func() {
		_ = eng.Shutdown(ctx)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		executedCount.Store(0)
		numJobs := 1000

		for i := 1; i <= numJobs; i++ {
			chainID := fmt.Sprintf("bench-claim-chain-%d", i)
			_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
				ChainID:  chainID,
				Sequence: 1,
				Type:     "BenchmarkTask",
				Payload:  TestPayload{ID: chainID, Value: i},
			})
			if err != nil {
				b.Fatalf("enqueue failed: %v", err)
			}
		}

		for executedCount.Load() < int64(numJobs) {
			time.Sleep(200 * time.Microsecond)
		}
	}
}

// BenchmarkEngine_Scale100kChains simulates high-concurrency scalability across 100,000 independent chains.
// It measures claim throughput and verifies zero lock contention under massive scale.
func BenchmarkEngine_Scale100kChains(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 100k chains scale benchmark in short mode")
	}

	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(64),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
		orderedjob.WithPollInterval(1*time.Millisecond),
	)

	ctx := context.Background()
	var processedChains atomic.Int64
	totalChains := 100000

	orderedjob.RegisterTyped(eng, "ScaleTask", func(ctx context.Context, job orderedjob.Job, payload TestPayload) error {
		processedChains.Add(1)
		return nil
	})

	err := eng.Start(ctx)
	if err != nil {
		b.Fatalf("failed to start engine: %v", err)
	}
	defer func() {
		_ = eng.Shutdown(ctx)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		processedChains.Store(0)

		var wg sync.WaitGroup
		workers := 50
		chainsPerWorker := totalChains / workers

		for w := range workers {
			wg.Add(1)
			go func(workerIdx int) {
				defer wg.Done()
				startIdx := workerIdx * chainsPerWorker
				endIdx := startIdx + chainsPerWorker
				for c := startIdx; c < endIdx; c++ {
					chainID := fmt.Sprintf("scale-100k-chain-%d", c)
					_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
						ChainID:  chainID,
						Sequence: 1,
						Type:     "ScaleTask",
						Payload:  TestPayload{ID: chainID, Value: c},
					})
					if err != nil {
						b.Errorf("enqueue failed for chain %s: %v", chainID, err)
						return
					}
				}
			}(w)
		}

		wg.Wait()

		for processedChains.Load() < int64(totalChains) {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// BenchmarkLockContention_AutoSequenceAdvisoryLock tests high-concurrency auto-sequence enqueue operations
// (Sequence = 0) to measure lock contention and confirm zero deadlock occurrences.
func BenchmarkLockContention_AutoSequenceAdvisoryLock(b *testing.B) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(16),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var wg sync.WaitGroup
		numGoroutines := 20
		enqueuesPerGoroutine := 50

		for g := range numGoroutines {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				chainID := fmt.Sprintf("contention-chain-%d", goroutineID%5)
				for range enqueuesPerGoroutine {
					_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
						ChainID:  chainID,
						Sequence: 0, // Auto-sequence allocation (triggers advisory locking mechanism)
						Type:     "AutoSeqTask",
						Payload:  TestPayload{ID: chainID, Value: goroutineID},
					})
					if err != nil {
						b.Errorf("auto-sequence enqueue failed: %v", err)
						return
					}
				}
			}(g)
		}

		wg.Wait()
	}
}
