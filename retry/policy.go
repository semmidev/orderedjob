package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy per Appendix B and Section 10.
type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      JitterMode
}

type JitterMode int

const (
	JitterNone JitterMode = iota
	JitterFull
	JitterEqual
)

func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		BaseDelay:   1 * time.Second,
		MaxDelay:    5 * time.Minute,
		Jitter:      JitterEqual,
	}
}

func (p Policy) Backoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	// base * 2^(attempt-1)
	backoff := float64(p.BaseDelay) * math.Pow(2, float64(attempt-1))
	if backoff > float64(p.MaxDelay) {
		backoff = float64(p.MaxDelay)
	}
	d := time.Duration(backoff)
	return applyJitter(d, p.Jitter)
}

func applyJitter(d time.Duration, mode JitterMode) time.Duration {
	switch mode {
	case JitterNone:
		return d
	case JitterFull:
		// 0..d
		// #nosec G404 - pseudo-random backoff jitter
		return time.Duration(rand.Float64() * float64(d))
	case JitterEqual:
		// d/2 + random(0,d/2)
		half := d / 2
		// #nosec G404 - pseudo-random backoff jitter
		return half + time.Duration(rand.Float64()*float64(half))
	default:
		return d
	}
}

// ShouldRetry determines if we have attempts left.
func (p Policy) ShouldRetry(attempt int) bool {
	return attempt < p.MaxAttempts
}
