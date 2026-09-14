package orderedjob

import "context"

// EventListener defines callbacks for key queue lifecycle events.
type EventListener interface {
	OnJobFailed(ctx context.Context, job Job, err error)
	OnChainBlocked(ctx context.Context, chainID string, job Job)
	OnDLQThresholdExceeded(ctx context.Context, dlqCount int64)
}

type NoopEventListener struct{}

func (NoopEventListener) OnJobFailed(context.Context, Job, error)       {}
func (NoopEventListener) OnChainBlocked(context.Context, string, Job)   {}
func (NoopEventListener) OnDLQThresholdExceeded(context.Context, int64) {}

// FunctionalEventListener allows registering individual functions as EventListener.
type FunctionalEventListener struct {
	OnJobFailedFunc            func(ctx context.Context, job Job, err error)
	OnChainBlockedFunc         func(ctx context.Context, chainID string, job Job)
	OnDLQThresholdExceededFunc func(ctx context.Context, dlqCount int64)
}

func (f *FunctionalEventListener) OnJobFailed(ctx context.Context, job Job, err error) {
	if f.OnJobFailedFunc != nil {
		f.OnJobFailedFunc(ctx, job, err)
	}
}

func (f *FunctionalEventListener) OnChainBlocked(ctx context.Context, chainID string, job Job) {
	if f.OnChainBlockedFunc != nil {
		f.OnChainBlockedFunc(ctx, chainID, job)
	}
}

func (f *FunctionalEventListener) OnDLQThresholdExceeded(ctx context.Context, dlqCount int64) {
	if f.OnDLQThresholdExceededFunc != nil {
		f.OnDLQThresholdExceededFunc(ctx, dlqCount)
	}
}
