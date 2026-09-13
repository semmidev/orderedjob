package clock

import "time"

// Clock abstracts time for testability. Production uses DB NOW() where consistency matters (C16),
// but this is for local decisions.
type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type FixedClock struct{ T time.Time }

func (f FixedClock) Now() time.Time { return f.T }
