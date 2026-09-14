package orderedjob

import (
	"context"
	"log/slog"
	"time"
)

// Metrics interface per proposal section 25.
type Metrics interface {
	IncClaimed()
	IncCompleted(jobType string)
	IncFailed(jobType string)
	IncRetry(jobType string)
	ObserveExecDuration(jobType string, d time.Duration)
	ObserveQueueDelay(jobType string, d time.Duration)
	SetActiveLeases(n int)
	IncClaimConflicts()
	IncStaleRecovered(n int)
	IncBlockedChain()
	IncPanics(jobType string)
}

type NoopMetrics struct{}

func (NoopMetrics) IncClaimed()                               {}
func (NoopMetrics) IncCompleted(string)                       {}
func (NoopMetrics) IncFailed(string)                          {}
func (NoopMetrics) IncRetry(string)                           {}
func (NoopMetrics) ObserveExecDuration(string, time.Duration) {}
func (NoopMetrics) ObserveQueueDelay(string, time.Duration)   {}
func (NoopMetrics) SetActiveLeases(int)                       {}
func (NoopMetrics) IncClaimConflicts()                        {}
func (NoopMetrics) IncStaleRecovered(int)                     {}
func (NoopMetrics) IncBlockedChain()                          {}
func (NoopMetrics) IncPanics(string)                          {}

// Logger structured minimal per section 26.
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	Debug(ctx context.Context, msg string, args ...any)
}

type SlogLogger struct{ L *slog.Logger }

func NewSlogLogger(l *slog.Logger) *SlogLogger {
	if l == nil {
		l = slog.Default()
	}
	return &SlogLogger{L: l}
}
func (s *SlogLogger) Info(ctx context.Context, msg string, args ...any) {
	s.L.InfoContext(ctx, msg, args...)
}
func (s *SlogLogger) Error(ctx context.Context, msg string, args ...any) {
	s.L.ErrorContext(ctx, msg, args...)
}
func (s *SlogLogger) Debug(ctx context.Context, msg string, args ...any) {
	s.L.DebugContext(ctx, msg, args...)
}

type NoopLogger struct{}

func (NoopLogger) Info(context.Context, string, ...any)  {}
func (NoopLogger) Error(context.Context, string, ...any) {}
func (NoopLogger) Debug(context.Context, string, ...any) {}

func LogJobAttrs(job Job) []any {
	return []any{
		"job_id", job.ID.String(),
		"chain_id", job.ChainID,
		"sequence", job.Sequence,
		"job_type", job.Type,
		"status", job.Status,
		"attempt", job.Attempt,
		"worker_id", job.WorkerID,
		"trace_id", job.TraceID,
	}
}

type traceIDKey struct{}

// WithTraceID attaches a trace ID to the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// ExtractTraceID extracts trace ID from context.
func ExtractTraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey{}).(string); ok {
		return v
	}
	return ""
}
