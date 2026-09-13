package lease

import (
	"time"
)

// Manager holds lease config per Appendix B.
type Manager struct {
	LeaseDuration time.Duration
	Heartbeat     time.Duration
}

func New(lease time.Duration) Manager {
	if lease <= 0 {
		lease = 60 * time.Second
	}
	return Manager{
		LeaseDuration: lease,
		Heartbeat:     lease / 3,
	}
}
