package orderedjob_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

func TestOrderingStrict(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(5), orderedjob.WithPollInterval(10*time.Millisecond), orderedjob.WithLogger(orderedjob.NoopLogger{}))

	var executed []string
	var mu struct{ m []string }
	ch := make(chan string, 100)

	eng.Register("test", orderedjob.HandlerFunc(func(ctx context.Context, job orderedjob.Job) error {
		ch <- job.ChainID + ":" + string(rune(job.Sequence+'0'))
		return nil
	}))

	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(ctx)

	for i := 1; i <= 5; i++ {
		_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "A", Sequence: int64(i), Type: "test", Payload: map[string]int{"i": i}})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	timeout := time.After(3 * time.Second)
	var results []string
	for len(results) < 5 {
		select {
		case r := <-ch:
			results = append(results, r)
		case <-timeout:
			t.Fatalf("timeout waiting, got %v", results)
		}
	}

	expected := []string{"A:1", "A:2", "A:3", "A:4", "A:5"}
	for i, exp := range expected {
		if results[i] != exp {
			t.Fatalf("ordering violation: expected %v got %v", expected, results)
		}
	}
	_ = executed
	_ = mu
	_ = uuid.New()
}

func TestConcurrentAcrossChains(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(10), orderedjob.WithPollInterval(10*time.Millisecond))

	ch := make(chan string, 100)
	eng.Register("test", orderedjob.HandlerFunc(func(ctx context.Context, job orderedjob.Job) error {
		time.Sleep(20 * time.Millisecond)
		ch <- job.ChainID
		return nil
	}))

	ctx := context.Background()
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	for _, chain := range []string{"X", "Y", "Z"} {
		for i := 1; i <= 3; i++ {
			_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: chain, Sequence: int64(i), Type: "test"})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	timeout := time.After(3 * time.Second)
	var got []string
	for len(got) < 9 {
		select {
		case r := <-ch:
			got = append(got, r)
		case <-timeout:
			t.Fatalf("timeout got %d", len(got))
		}
	}
	if len(got) != 9 {
		t.Fatalf("expected 9, got %d", len(got))
	}
}

func TestRetryAndLeaseRecovery(t *testing.T) {
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(1), orderedjob.WithPollInterval(10*time.Millisecond), orderedjob.WithLease(100*time.Millisecond), orderedjob.WithRecoveryInterval(50*time.Millisecond))

	attempts := 0
	ch := make(chan int, 10)
	eng.Register("retry", orderedjob.HandlerFunc(func(ctx context.Context, job orderedjob.Job) error {
		attempts++
		if attempts < 3 {
			return orderedjob.Retryable(assertErr("transient"))
		}
		ch <- attempts
		return nil
	}))

	ctx := context.Background()
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "R", Type: "retry"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case a := <-ch:
		if a != 3 {
			t.Fatalf("expected 3 attempts, got %d", a)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout retry")
	}
}

func assertErr(s string) error { return &testErr{s} }

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }
