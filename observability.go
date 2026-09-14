package orderedjob

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
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

// OTelMetrics implements Metrics backed by OpenTelemetry Metrics API.
type OTelMetrics struct {
	meter          metric.Meter
	jobsClaimed    metric.Int64Counter
	jobsCompleted  metric.Int64Counter
	jobsFailed     metric.Int64Counter
	jobsRetried    metric.Int64Counter
	jobsPanics     metric.Int64Counter
	claimConflicts metric.Int64Counter
	staleRecovered metric.Int64Counter
	blockedChains  metric.Int64Counter
	execDuration   metric.Float64Histogram
	queueDelay     metric.Float64Histogram
	activeLeases   metric.Int64Gauge
}

// NewOTelMetrics creates a Metrics exporter backed by an OpenTelemetry Meter.
// If mp is nil, otel.GetMeterProvider() is used.
func NewOTelMetrics(mp metric.MeterProvider) (*OTelMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter("github.com/semmidev/orderedjob")

	claimed, err := meter.Int64Counter("orderedjob_jobs_claimed_total",
		metric.WithDescription("Total number of jobs claimed by workers"))
	if err != nil {
		return nil, err
	}

	completed, err := meter.Int64Counter("orderedjob_jobs_completed_total",
		metric.WithDescription("Total number of jobs completed successfully"))
	if err != nil {
		return nil, err
	}

	failed, err := meter.Int64Counter("orderedjob_jobs_failed_total",
		metric.WithDescription("Total number of jobs failed terminally"))
	if err != nil {
		return nil, err
	}

	retried, err := meter.Int64Counter("orderedjob_job_retries_total",
		metric.WithDescription("Total number of job retries"))
	if err != nil {
		return nil, err
	}

	panics, err := meter.Int64Counter("orderedjob_panics_total",
		metric.WithDescription("Total number of job handler panics caught"))
	if err != nil {
		return nil, err
	}

	conflicts, err := meter.Int64Counter("orderedjob_claim_conflicts_total",
		metric.WithDescription("Total number of lease claim / fencing conflicts"))
	if err != nil {
		return nil, err
	}

	recovered, err := meter.Int64Counter("orderedjob_stale_recovered_total",
		metric.WithDescription("Total number of stale expired leases recovered"))
	if err != nil {
		return nil, err
	}

	blocked, err := meter.Int64Counter("orderedjob_blocked_chains_total",
		metric.WithDescription("Total number of chain blockages"))
	if err != nil {
		return nil, err
	}

	execDur, err := meter.Float64Histogram("orderedjob_job_execution_duration_seconds",
		metric.WithDescription("Job execution duration in seconds"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	qDelay, err := meter.Float64Histogram("orderedjob_queue_delay_seconds",
		metric.WithDescription("Queue delay / latency distribution in seconds"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	leases, err := meter.Int64Gauge("orderedjob_active_leases",
		metric.WithDescription("Current active lease count"))
	if err != nil {
		return nil, err
	}

	return &OTelMetrics{
		meter:          meter,
		jobsClaimed:    claimed,
		jobsCompleted:  completed,
		jobsFailed:     failed,
		jobsRetried:    retried,
		jobsPanics:     panics,
		claimConflicts: conflicts,
		staleRecovered: recovered,
		blockedChains:  blocked,
		execDuration:   execDur,
		queueDelay:     qDelay,
		activeLeases:   leases,
	}, nil
}

func (m *OTelMetrics) IncClaimed() {
	m.jobsClaimed.Add(context.Background(), 1)
}

func (m *OTelMetrics) IncCompleted(jobType string) {
	m.jobsCompleted.Add(context.Background(), 1, metric.WithAttributes(attribute.String("job_type", jobType)))
}

func (m *OTelMetrics) IncFailed(jobType string) {
	m.jobsFailed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("job_type", jobType)))
}

func (m *OTelMetrics) IncRetry(jobType string) {
	m.jobsRetried.Add(context.Background(), 1, metric.WithAttributes(attribute.String("job_type", jobType)))
}

func (m *OTelMetrics) ObserveExecDuration(jobType string, d time.Duration) {
	m.execDuration.Record(context.Background(), d.Seconds(), metric.WithAttributes(attribute.String("job_type", jobType)))
}

func (m *OTelMetrics) ObserveQueueDelay(jobType string, d time.Duration) {
	m.queueDelay.Record(context.Background(), d.Seconds(), metric.WithAttributes(attribute.String("job_type", jobType)))
}

func (m *OTelMetrics) SetActiveLeases(n int) {
	m.activeLeases.Record(context.Background(), int64(n))
}

func (m *OTelMetrics) IncClaimConflicts() {
	m.claimConflicts.Add(context.Background(), 1)
}

func (m *OTelMetrics) IncStaleRecovered(n int) {
	m.staleRecovered.Add(context.Background(), int64(n))
}

func (m *OTelMetrics) IncBlockedChain() {
	m.blockedChains.Add(context.Background(), 1)
}

func (m *OTelMetrics) IncPanics(jobType string) {
	m.jobsPanics.Add(context.Background(), 1, metric.WithAttributes(attribute.String("job_type", jobType)))
}

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

// InjectOTelTraceContext extracts W3C traceparent (or OTel SpanContext) from ctx
// and sets req.TraceID if req.TraceID is empty.
func InjectOTelTraceContext(ctx context.Context, req *EnqueueRequest) {
	if req.TraceID != "" {
		return
	}
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		req.TraceID = spanCtx.TraceID().String()
		return
	}
	if tID := ExtractTraceID(ctx); tID != "" {
		req.TraceID = tID
	}
}

// ExtractOTelTraceContext extracts TraceID from job into context.
func ExtractOTelTraceContext(ctx context.Context, job Job) context.Context {
	if job.TraceID == "" {
		return ctx
	}
	return WithTraceID(ctx, job.TraceID)
}
