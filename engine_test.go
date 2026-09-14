package orderedjob_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
	"github.com/semmidev/orderedjob/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type TestPayload struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

func TestEngine_EnqueueValidation(t *testing.T) {
	tests := []struct {
		name      string
		req       orderedjob.EnqueueRequest
		expectErr bool
		errString string
	}{
		{
			name: "valid explicit sequence enqueue",
			req: orderedjob.EnqueueRequest{
				ChainID:  "chain-1",
				Sequence: 1,
				Type:     "ProcessItem",
				Payload:  TestPayload{ID: "item-1", Value: 100},
			},
			expectErr: false,
		},
		{
			name: "valid auto sequence enqueue (sequence 0)",
			req: orderedjob.EnqueueRequest{
				ChainID:  "chain-2",
				Sequence: 0,
				Type:     "ProcessItem",
				Payload:  TestPayload{ID: "item-2", Value: 200},
			},
			expectErr: false,
		},
		{
			name: "missing chain ID",
			req: orderedjob.EnqueueRequest{
				ChainID:  "",
				Sequence: 1,
				Type:     "ProcessItem",
			},
			expectErr: true,
			errString: "chain_id required",
		},
		{
			name: "missing job type",
			req: orderedjob.EnqueueRequest{
				ChainID:  "chain-3",
				Sequence: 1,
				Type:     "",
			},
			expectErr: true,
			errString: "job type required",
		},
		{
			name: "invalid sequence gap (first sequence must be 1)",
			req: orderedjob.EnqueueRequest{
				ChainID:  "chain-gap",
				Sequence: 5,
				Type:     "ProcessItem",
			},
			expectErr: true,
			errString: "sequence gap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.New()
			eng := orderedjob.New(repo, orderedjob.WithLogger(orderedjob.NoopLogger{}))
			ctx := context.Background()

			job, err := eng.Enqueue(ctx, tt.req)

			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errString)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, job.ID)
				assert.Equal(t, tt.req.ChainID, job.ChainID)
				assert.Equal(t, tt.req.Type, job.Type)
			}
		})
	}
}

func TestEngine_StrictFIFOOrdering(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(5),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var mu sync.Mutex
	var executed []string

	orderedjob.RegisterTyped(eng, "ProcessStep", func(ctx context.Context, job orderedjob.Job, payload TestPayload) error {
		mu.Lock()
		executed = append(executed, fmt.Sprintf("%s:%d", job.ChainID, job.Sequence))
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	err := eng.Start(ctx)
	require.NoError(t, err)
	defer func() {
		_ = eng.Shutdown(ctx)
	}()

	// Enqueue 5 jobs in chain "chain-fifo"
	for seq := 1; seq <= 5; seq++ {
		_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-fifo",
			Sequence: int64(seq),
			Type:     "ProcessStep",
			Payload:  TestPayload{ID: "step", Value: seq},
		})
		require.NoError(t, err)
	}

	// Assert order of execution
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(executed) == 5
	}, 3*time.Second, 20*time.Millisecond, "expected 5 jobs to complete")

	mu.Lock()
	defer mu.Unlock()
	expected := []string{"chain-fifo:1", "chain-fifo:2", "chain-fifo:3", "chain-fifo:4", "chain-fifo:5"}
	assert.Equal(t, expected, executed, "FIFO sequence order must be strictly preserved")
}

func TestEngine_BatchEnqueue(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithLogger(orderedjob.NoopLogger{}))
	ctx := context.Background()

	batchReqs := []orderedjob.EnqueueRequest{
		{ChainID: "batch-chain", Sequence: 0, Type: "TaskA", Payload: TestPayload{ID: "1"}},
		{ChainID: "batch-chain", Sequence: 0, Type: "TaskB", Payload: TestPayload{ID: "2"}},
		{ChainID: "batch-chain", Sequence: 0, Type: "TaskC", Payload: TestPayload{ID: "3"}},
	}

	jobs, err := eng.EnqueueBatch(ctx, batchReqs)
	require.NoError(t, err)
	require.Len(t, jobs, 3)

	assert.Equal(t, int64(1), jobs[0].Sequence)
	assert.Equal(t, int64(2), jobs[1].Sequence)
	assert.Equal(t, int64(3), jobs[2].Sequence)
}

func TestEngine_RetryableAndPermanentError(t *testing.T) {
	tests := []struct {
		name              string
		handlerFn         func(attempts *atomic.Int32) func(ctx context.Context, job orderedjob.Job) error
		expectedAttempts  int32
		expectedCompleted bool
	}{
		{
			name: "transient error retries until success",
			handlerFn: func(attempts *atomic.Int32) func(ctx context.Context, job orderedjob.Job) error {
				return func(ctx context.Context, job orderedjob.Job) error {
					count := attempts.Add(1)
					if count < 3 {
						return orderedjob.Retryable(errors.New("temporary network timeout"))
					}
					return nil
				}
			},
			expectedAttempts:  3,
			expectedCompleted: true,
		},
		{
			name: "permanent non-retryable error stops immediately",
			handlerFn: func(attempts *atomic.Int32) func(ctx context.Context, job orderedjob.Job) error {
				return func(ctx context.Context, job orderedjob.Job) error {
					attempts.Add(1)
					return orderedjob.NonRetryable(errors.New("unrecoverable validation error"))
				}
			},
			expectedAttempts:  1,
			expectedCompleted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.New()
			eng := orderedjob.New(repo,
				orderedjob.WithConcurrency(1),
				orderedjob.WithPollInterval(10*time.Millisecond),
				orderedjob.WithLogger(orderedjob.NoopLogger{}),
				orderedjob.WithRetryPolicy(retry.Policy{
					MaxAttempts: 3,
					BaseDelay:   10 * time.Millisecond,
					MaxDelay:    50 * time.Millisecond,
				}),
			)

			var attempts atomic.Int32
			doneCh := make(chan bool, 1)

			eng.RegisterFunc("TestJob", func(ctx context.Context, job orderedjob.Job) error {
				err := tt.handlerFn(&attempts)(ctx, job)
				if err == nil {
					doneCh <- true
				}
				return err
			})

			ctx := context.Background()
			err := eng.Start(ctx)
			require.NoError(t, err)
			defer func() { _ = eng.Shutdown(ctx) }()

			_, err = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
				ChainID:  "retry-chain-" + tt.name,
				Sequence: 1,
				Type:     "TestJob",
			})
			require.NoError(t, err)

			if tt.expectedCompleted {
				select {
				case <-doneCh:
					assert.Equal(t, tt.expectedAttempts, attempts.Load())
				case <-time.After(2 * time.Second):
					t.Fatal("timed out waiting for job completion")
				}
			} else {
				time.Sleep(150 * time.Millisecond)
				assert.Equal(t, tt.expectedAttempts, attempts.Load())
			}
		})
	}
}

func TestEngine_TraceIDContextPropagation(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	capturedTraceID := make(chan string, 1)
	orderedjob.RegisterTyped(eng, "TraceTest", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		capturedTraceID <- traceID
		return nil
	})

	ctx := context.Background()
	err := eng.Start(ctx)
	require.NoError(t, err)
	defer func() { _ = eng.Shutdown(ctx) }()

	expectedTraceID := "trace-xyz-12345"
	ctx = orderedjob.WithTraceID(ctx, expectedTraceID)

	_, err = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "trace-chain",
		Sequence: 1,
		Type:     "TraceTest",
		Payload:  map[string]string{"foo": "bar"},
	})
	require.NoError(t, err)

	select {
	case traceID := <-capturedTraceID:
		assert.Equal(t, expectedTraceID, traceID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trace_id extraction")
	}
}

func TestEngine_OrderingMode_Strict(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithOrderingMode(orderedjob.OrderingModeStrict),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var job1Attempted, job2Attempted bool
	var mu sync.Mutex

	eng.RegisterFunc("FailingJob", func(ctx context.Context, job orderedjob.Job) error {
		mu.Lock()
		job1Attempted = true
		mu.Unlock()
		return orderedjob.NonRetryable(errors.New("fatal job error"))
	})
	eng.RegisterFunc("NextJob", func(ctx context.Context, job orderedjob.Job) error {
		mu.Lock()
		job2Attempted = true
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	j1, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-strict", Sequence: 1, Type: "FailingJob"})
	require.NoError(t, err)
	j2, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-strict", Sequence: 2, Type: "NextJob"})
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	assert.True(t, job1Attempted)
	assert.False(t, job2Attempted, "Job 2 must NOT execute under strict mode when Job 1 fails terminally")
	mu.Unlock()

	j1Fetched, _ := eng.Get(ctx, j1.ID)
	j2Fetched, _ := eng.Get(ctx, j2.ID)
	assert.Equal(t, "FAILED", j1Fetched.Status)
	assert.Equal(t, "BLOCKED", j2Fetched.Status)
}

func TestEngine_OrderingMode_SkipOnFailure(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithOrderingMode(orderedjob.OrderingModeSkipOnFailure),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var job2Executed = make(chan struct{}, 1)

	eng.RegisterFunc("FailingJob", func(ctx context.Context, job orderedjob.Job) error {
		return orderedjob.NonRetryable(errors.New("fatal job error"))
	})
	eng.RegisterFunc("NextJob", func(ctx context.Context, job orderedjob.Job) error {
		select {
		case job2Executed <- struct{}{}:
		default:
		}
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	j1, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-skip", Sequence: 1, Type: "FailingJob"})
	require.NoError(t, err)
	j2, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-skip", Sequence: 2, Type: "NextJob"})
	require.NoError(t, err)

	select {
	case <-job2Executed:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Job 2 failed to execute after Job 1 failed under skip-on-failure mode")
	}

	j1Fetched, _ := eng.Get(ctx, j1.ID)
	j2Fetched, _ := eng.Get(ctx, j2.ID)
	assert.Equal(t, "FAILED", j1Fetched.Status)
	assert.Equal(t, "COMPLETED", j2Fetched.Status)
}

func TestEngine_OrderingMode_DeadLetterAndContinue(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithOrderingMode(orderedjob.OrderingModeDeadLetterAndContinue),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var job2Executed = make(chan struct{}, 1)

	eng.RegisterFunc("FailingJob", func(ctx context.Context, job orderedjob.Job) error {
		return orderedjob.NonRetryable(errors.New("fatal job error"))
	})
	eng.RegisterFunc("NextJob", func(ctx context.Context, job orderedjob.Job) error {
		select {
		case job2Executed <- struct{}{}:
		default:
		}
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	j1, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-dlq", Sequence: 1, Type: "FailingJob"})
	require.NoError(t, err)
	j2, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-dlq", Sequence: 2, Type: "NextJob"})
	require.NoError(t, err)

	select {
	case <-job2Executed:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Job 2 failed to execute under dead-letter-and-continue mode")
	}

	j1Fetched, _ := eng.Get(ctx, j1.ID)
	j2Fetched, _ := eng.Get(ctx, j2.ID)
	assert.Equal(t, "DEAD_LETTERED", j1Fetched.Status)
	assert.Equal(t, "COMPLETED", j2Fetched.Status)
}

func TestEngine_OrderingStrategyResolver(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithOrderingStrategy(func(req orderedjob.EnqueueRequest) string {
			if req.Type == "SkipType" {
				return orderedjob.OrderingModeSkipOnFailure
			}
			return orderedjob.OrderingModeStrict
		}),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var job2Executed = make(chan struct{}, 1)

	eng.RegisterFunc("SkipType", func(ctx context.Context, job orderedjob.Job) error {
		return orderedjob.NonRetryable(errors.New("fatal error"))
	})
	eng.RegisterFunc("NextJob", func(ctx context.Context, job orderedjob.Job) error {
		select {
		case job2Executed <- struct{}{}:
		default:
		}
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	j1, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-dyn", Sequence: 1, Type: "SkipType"})
	require.NoError(t, err)
	assert.Equal(t, orderedjob.OrderingModeSkipOnFailure, j1.OrderingMode)

	j2, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-dyn", Sequence: 2, Type: "NextJob"})
	require.NoError(t, err)

	select {
	case <-job2Executed:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Job 2 failed to execute when Job 1 resolved to skip-on-failure via resolver strategy")
	}

	j2Fetched, _ := eng.Get(ctx, j2.ID)
	assert.Equal(t, "COMPLETED", j2Fetched.Status)
}

func TestEngine_ScheduledAndDelayedJobs(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithPollInterval(20*time.Millisecond),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	executed := make(chan string, 10)
	eng.RegisterFunc("ScheduledType", func(ctx context.Context, job orderedjob.Job) error {
		executed <- job.ChainID
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	// 1. Enqueue a job delayed by 200ms
	futureTime := time.Now().Add(200 * time.Millisecond)
	j1, err := eng.Schedule(ctx, orderedjob.EnqueueRequest{
		ChainID:  "c-scheduled-1",
		Sequence: 1,
		Type:     "ScheduledType",
	}, futureTime)
	require.NoError(t, err)
	assert.True(t, j1.AvailableAt.After(time.Now().Add(100*time.Millisecond)))

	// 2. Enqueue via EnqueueDelayed helper (300ms)
	j2, err := eng.EnqueueDelayed(ctx, orderedjob.EnqueueRequest{
		ChainID:  "c-scheduled-2",
		Sequence: 1,
		Type:     "ScheduledType",
	}, 300*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, j2.AvailableAt.After(time.Now().Add(200*time.Millisecond)))

	// Verify stats count scheduled jobs correctly
	stats, err := repo.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.Scheduled)

	// Verify ListJobs with ScheduledOnly filter
	scheduledJobs, total, err := repo.ListJobs(ctx, orderedjob.JobFilter{ScheduledOnly: true})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, scheduledJobs, 2)

	// Ensure job isn't executed immediately
	select {
	case id := <-executed:
		t.Fatalf("Job executed prematurely before available_at: %s", id)
	case <-time.After(50 * time.Millisecond):
		// Expected: not executed yet
	}

	// Wait for delayed jobs to execute
	time.Sleep(350 * time.Millisecond)

	select {
	case <-executed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Scheduled job 1 did not execute after available_at")
	}

	select {
	case <-executed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Delayed job 2 did not execute after available_at")
	}
}

func TestEngine_RescheduleJob(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(20*time.Millisecond),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	executed := make(chan struct{}, 1)
	eng.RegisterFunc("RescheduledJob", func(ctx context.Context, job orderedjob.Job) error {
		executed <- struct{}{}
		return nil
	})

	ctx := context.Background()
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	// Enqueue far in future (1 hour)
	farFuture := time.Now().Add(1 * time.Hour)
	j, err := eng.Schedule(ctx, orderedjob.EnqueueRequest{
		ChainID:  "c-resched",
		Sequence: 1,
		Type:     "RescheduledJob",
	}, farFuture)
	require.NoError(t, err)

	// Reschedule to run soon (50ms from now)
	nearFuture := time.Now().Add(50 * time.Millisecond)
	updatedJob, err := eng.RescheduleJob(ctx, j.ID, nearFuture)
	require.NoError(t, err)
	assert.WithinDuration(t, nearFuture, updatedJob.AvailableAt, 10*time.Millisecond)

	// Wait for execution
	select {
	case <-executed:
		// Success!
	case <-time.After(2 * time.Second):
		t.Fatal("Rescheduled job was not claimed/executed in time")
	}
}

func TestEngine_WorkerPanicRecoveryGuard(t *testing.T) {
	t.Run("Default panic isolation marks job FAILED and keeps worker pool alive", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithOrderingMode(orderedjob.OrderingModeSkipOnFailure),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		job2Executed := make(chan struct{}, 1)

		eng.RegisterFunc("PanickingJob", func(ctx context.Context, job orderedjob.Job) error {
			panic("simulated unhandled runtime panic in handler")
		})
		eng.RegisterFunc("NextHealthyJob", func(ctx context.Context, job orderedjob.Job) error {
			job2Executed <- struct{}{}
			return nil
		})

		ctx := context.Background()
		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		j1, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-panic", Sequence: 1, Type: "PanickingJob"})
		require.NoError(t, err)

		j2, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-panic", Sequence: 2, Type: "NextHealthyJob"})
		require.NoError(t, err)

		// Wait for job2 to be executed despite job1 panicking
		select {
		case <-job2Executed:
			// Success - worker pool survived the panic!
		case <-time.After(2 * time.Second):
			t.Fatal("Worker pool crashed or stopped processing after handler panic")
		}

		j1Fetched, _ := eng.Get(ctx, j1.ID)
		j2Fetched, _ := eng.Get(ctx, j2.ID)

		assert.Equal(t, "FAILED", j1Fetched.Status)
		assert.Contains(t, j1Fetched.LastError, "panic: simulated unhandled runtime panic")
		assert.Contains(t, j1Fetched.LastError, "stacktrace:")
		assert.Equal(t, "COMPLETED", j2Fetched.Status)
	})

	t.Run("Custom PanicHandler interceptor", func(t *testing.T) {
		repo := memory.New()
		var mu sync.Mutex
		var interceptedPanic any

		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithPanicHandler(func(ctx context.Context, job orderedjob.Job, panicVal any, stack []byte) error {
				mu.Lock()
				interceptedPanic = panicVal
				mu.Unlock()
				return orderedjob.NonRetryable(fmt.Errorf("custom panic handling: %v", panicVal))
			}),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		eng.RegisterFunc("PanickingJob", func(ctx context.Context, job orderedjob.Job) error {
			panic("custom panic payload")
		})

		ctx := context.Background()
		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		j, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-panic-custom", Sequence: 1, Type: "PanickingJob"})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		val := interceptedPanic
		mu.Unlock()
		assert.Equal(t, "custom panic payload", val)

		jFetched, _ := eng.Get(ctx, j.ID)
		assert.Equal(t, "FAILED", jFetched.Status)
		assert.Contains(t, jFetched.LastError, "custom panic handling: custom panic payload")
	})

	t.Run("WithRetryOnPanic enables retry behavior", func(t *testing.T) {
		repo := memory.New()
		var attempts atomic.Int32

		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithRetryOnPanic(true),
			orderedjob.WithRetryPolicy(retry.Policy{
				MaxAttempts: 3,
				BaseDelay:   10 * time.Millisecond,
				MaxDelay:    50 * time.Millisecond,
			}),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		eng.RegisterFunc("FlakyPanicJob", func(ctx context.Context, job orderedjob.Job) error {
			if attempts.Add(1) < 2 {
				panic("transient panic")
			}
			return nil
		})

		ctx := context.Background()
		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		j, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-panic-retry", Sequence: 1, Type: "FlakyPanicJob"})
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		jFetched, _ := eng.Get(ctx, j.ID)
		assert.Equal(t, "COMPLETED", jFetched.Status)
		assert.Equal(t, int32(2), attempts.Load())
	})
}

type ValidatablePayload struct {
	Age  int    `json:"age"`
	Name string `json:"name"`
}

func (v ValidatablePayload) Validate() error {
	if v.Age < 18 {
		return fmt.Errorf("age must be at least 18")
	}
	if v.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	return nil
}

type ValidatableCtxPayload struct {
	Role string `json:"role"`
}

func (v ValidatableCtxPayload) ValidateCtx(ctx context.Context) error {
	if v.Role != "admin" {
		return fmt.Errorf("role must be admin")
	}
	return nil
}

func TestEngine_TypedHandlerValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("Self-validation via Validator interface failure", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		var handled bool
		orderedjob.RegisterTyped(eng, "UserJob", func(ctx context.Context, job orderedjob.Job, payload ValidatablePayload) error {
			handled = true
			return nil
		})

		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		// Invalid payload (age < 18)
		j, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-val-1",
			Sequence: 1,
			Type:     "UserJob",
			Payload:  ValidatablePayload{Age: 16, Name: "John"},
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		jFetched, err := eng.Get(ctx, j.ID)
		require.NoError(t, err)
		assert.Equal(t, "FAILED", jFetched.Status)
		assert.False(t, handled)
		assert.Contains(t, jFetched.LastError, "payload validation failed")
		assert.Contains(t, jFetched.LastError, "age must be at least 18")
	})

	t.Run("Self-validation via ValidatorCtx interface success and failure", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		var handled bool
		orderedjob.RegisterTyped(eng, "AdminJob", func(ctx context.Context, job orderedjob.Job, payload ValidatableCtxPayload) error {
			handled = true
			return nil
		})

		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		// Invalid role
		j, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-val-2",
			Sequence: 1,
			Type:     "AdminJob",
			Payload:  ValidatableCtxPayload{Role: "user"},
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		jFetched, err := eng.Get(ctx, j.ID)
		require.NoError(t, err)
		assert.Equal(t, "FAILED", jFetched.Status)
		assert.False(t, handled)
		assert.Contains(t, jFetched.LastError, "role must be admin")
	})

	t.Run("Custom validator function via RegisterTypedWithValidator", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo,
			orderedjob.WithConcurrency(1),
			orderedjob.WithPollInterval(10*time.Millisecond),
			orderedjob.WithLogger(orderedjob.NoopLogger{}),
		)

		var handled bool
		orderedjob.RegisterTypedWithValidator(eng, "CustomValJob",
			func(ctx context.Context, job orderedjob.Job, payload map[string]int) error {
				handled = true
				return nil
			},
			func(ctx context.Context, payload map[string]int) error {
				if payload["amount"] <= 0 {
					return fmt.Errorf("amount must be positive")
				}
				return nil
			},
		)

		require.NoError(t, eng.Start(ctx))
		defer func() { _ = eng.Shutdown(ctx) }()

		// Invalid amount <= 0
		j, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-val-3",
			Sequence: 1,
			Type:     "CustomValJob",
			Payload:  map[string]int{"amount": -50},
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		jFetched, err := eng.Get(ctx, j.ID)
		require.NoError(t, err)
		assert.Equal(t, "FAILED", jFetched.Status)
		assert.False(t, handled)
		assert.Contains(t, jFetched.LastError, "amount must be positive")
	})
}

func TestEngine_OpenTelemetryTracing(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)))
	tracer := tp.Tracer("test-tracer")

	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(1),
		orderedjob.WithPollInterval(10*time.Millisecond),
		orderedjob.WithTracerProvider(tp),
		orderedjob.WithLogger(orderedjob.NoopLogger{}),
	)

	var handlerExecuted atomic.Bool
	var handlerTraceID atomic.Value
	eng.RegisterFunc("OTelJob", func(ctx context.Context, job orderedjob.Job) error {
		handlerExecuted.Store(true)
		handlerTraceID.Store(orderedjob.ExtractTraceID(ctx))
		return nil
	})

	ctx := context.Background()
	parentCtx, parentSpan := tracer.Start(ctx, "producer-span")

	job, err := eng.Enqueue(parentCtx, orderedjob.EnqueueRequest{
		ChainID:  "otel-chain",
		Sequence: 1,
		Type:     "OTelJob",
	})
	require.NoError(t, err)
	parentSpan.End()

	require.NotEmpty(t, job.TraceID)
	require.Equal(t, parentSpan.SpanContext().TraceID().String(), job.TraceID)

	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Shutdown(ctx) }()

	time.Sleep(150 * time.Millisecond)

	assert.True(t, handlerExecuted.Load())
	gotTraceID, _ := handlerTraceID.Load().(string)
	assert.Equal(t, job.TraceID, gotTraceID)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)

	var foundConsumerSpan bool
	for _, s := range spans {
		if s.Name == "orderedjob.execute" {
			foundConsumerSpan = true
			assert.Equal(t, parentSpan.SpanContext().TraceID().String(), s.SpanContext.TraceID().String())
		}
	}
	assert.True(t, foundConsumerSpan, "consumer span orderedjob.execute should be exported")
}

func TestEngine_BulkDLQOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("BulkReplayDLQ", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo)

		j1, _ := repo.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-replay-1", Sequence: 1, Type: "ReplayType"})
		claimed, _ := repo.Claim(ctx, "w1", 10*time.Second)
		_ = repo.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "err", true)

		replayed, err := eng.BulkReplayDLQ(ctx, orderedjob.JobFilter{JobType: "ReplayType"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), replayed)

		jCheck, _ := eng.Get(ctx, j1.ID)
		assert.Equal(t, "PENDING", jCheck.Status)
	})

	t.Run("BulkSkipDLQ", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo)

		j2, _ := repo.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-skip-1", Sequence: 1, Type: "SkipType"})
		claimed, _ := repo.Claim(ctx, "w1", 10*time.Second)
		_ = repo.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "err", true)

		skipped, err := eng.BulkSkipDLQ(ctx, orderedjob.JobFilter{JobType: "SkipType"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), skipped)

		jCheck, _ := eng.Get(ctx, j2.ID)
		assert.Equal(t, "COMPLETED", jCheck.Status)
	})

	t.Run("BulkPurgeDLQ", func(t *testing.T) {
		repo := memory.New()
		eng := orderedjob.New(repo)

		j3, _ := repo.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "c-purge-1", Sequence: 1, Type: "PurgeType"})
		claimed, _ := repo.Claim(ctx, "w1", 10*time.Second)
		_ = repo.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "err", true)

		purged, err := eng.BulkPurgeDLQ(ctx, orderedjob.JobFilter{JobType: "PurgeType"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), purged)

		_, err = eng.Get(ctx, j3.ID)
		assert.ErrorIs(t, err, orderedjob.ErrNotFound)
	})
}
