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
