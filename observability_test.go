package orderedjob_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/semmidev/orderedjob"
)

func TestOTelMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := orderedjob.NewOTelMetrics(provider)
	require.NoError(t, err)

	metrics.IncClaimed()
	metrics.IncCompleted("send_email")
	metrics.IncFailed("send_email")
	metrics.IncRetry("send_email")
	metrics.IncPanics("send_email")
	metrics.IncClaimConflicts()
	metrics.IncStaleRecovered(2)
	metrics.IncBlockedChain()
	metrics.ObserveExecDuration("send_email", 150*time.Millisecond)
	metrics.ObserveQueueDelay("send_email", 50*time.Millisecond)
	metrics.SetActiveLeases(3)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	foundMetrics := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			foundMetrics[m.Name] = true
		}
	}

	expectedNames := []string{
		"orderedjob_jobs_claimed_total",
		"orderedjob_jobs_completed_total",
		"orderedjob_jobs_failed_total",
		"orderedjob_job_retries_total",
		"orderedjob_panics_total",
		"orderedjob_claim_conflicts_total",
		"orderedjob_stale_recovered_total",
		"orderedjob_blocked_chains_total",
		"orderedjob_job_execution_duration_seconds",
		"orderedjob_queue_delay_seconds",
		"orderedjob_active_leases",
	}

	for _, name := range expectedNames {
		require.True(t, foundMetrics[name], "metric %s should be recorded", name)
	}
}

func TestTraceContextPropagation(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)))
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "parent-span")
	defer span.End()

	req := orderedjob.EnqueueRequest{
		Type:    "process_payment",
		ChainID: "chain-1",
		Payload: []byte("{}"),
	}

	orderedjob.InjectOTelTraceContext(ctx, &req)
	require.NotEmpty(t, req.TraceID)
	require.Equal(t, span.SpanContext().TraceID().String(), req.TraceID)

	job := orderedjob.Job{
		TraceID: req.TraceID,
	}

	hctx := orderedjob.ExtractOTelTraceContext(context.Background(), job)
	require.Equal(t, req.TraceID, orderedjob.ExtractTraceID(hctx))
}
