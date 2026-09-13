package retry_test

import (
	"testing"
	"time"

	"github.com/semmidev/orderedjob/retry"
	"github.com/stretchr/testify/assert"
)

func TestPolicy_BackoffAndShouldRetry(t *testing.T) {
	tests := []struct {
		name          string
		policy        retry.Policy
		attempt       int
		expectedMin   time.Duration
		expectedMax   time.Duration
		expectedRetry bool
	}{
		{
			name:          "default policy initial attempt without jitter",
			policy:        retry.Policy{MaxAttempts: 5, BaseDelay: 1 * time.Second, MaxDelay: 10 * time.Second, Jitter: retry.JitterNone},
			attempt:       1,
			expectedMin:   1 * time.Second,
			expectedMax:   1 * time.Second,
			expectedRetry: true,
		},
		{
			name:          "exponential growth attempt 3 without jitter",
			policy:        retry.Policy{MaxAttempts: 5, BaseDelay: 1 * time.Second, MaxDelay: 10 * time.Second, Jitter: retry.JitterNone},
			attempt:       3,
			expectedMin:   4 * time.Second, // 1 * 2^(3-1) = 4s
			expectedMax:   4 * time.Second,
			expectedRetry: true,
		},
		{
			name:          "capped at max delay",
			policy:        retry.Policy{MaxAttempts: 5, BaseDelay: 1 * time.Second, MaxDelay: 5 * time.Second, Jitter: retry.JitterNone},
			attempt:       10,
			expectedMin:   5 * time.Second,
			expectedMax:   5 * time.Second,
			expectedRetry: false,
		},
		{
			name:          "equal jitter range check",
			policy:        retry.Policy{MaxAttempts: 3, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second, Jitter: retry.JitterEqual},
			attempt:       1,
			expectedMin:   1 * time.Second, // half=1s + (0..1s) => 1s..2s
			expectedMax:   2 * time.Second,
			expectedRetry: true,
		},
		{
			name:          "full jitter range check",
			policy:        retry.Policy{MaxAttempts: 3, BaseDelay: 4 * time.Second, MaxDelay: 10 * time.Second, Jitter: retry.JitterFull},
			attempt:       1,
			expectedMin:   0 * time.Second, // 0..4s
			expectedMax:   4 * time.Second,
			expectedRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := tt.policy.Backoff(tt.attempt)
			assert.GreaterOrEqual(t, backoff, tt.expectedMin, "backoff duration must be >= expectedMin")
			assert.LessOrEqual(t, backoff, tt.expectedMax, "backoff duration must be <= expectedMax")

			shouldRetry := tt.policy.ShouldRetry(tt.attempt)
			assert.Equal(t, tt.expectedRetry, shouldRetry)
		})
	}
}

func TestDefaultPolicy(t *testing.T) {
	p := retry.DefaultPolicy()
	assert.Equal(t, 5, p.MaxAttempts)
	assert.Equal(t, 1*time.Second, p.BaseDelay)
	assert.Equal(t, 5*time.Minute, p.MaxDelay)
	assert.Equal(t, retry.JitterEqual, p.Jitter)
}
