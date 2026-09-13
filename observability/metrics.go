package observability

import (
	"log/slog"

	orderedjob "github.com/semmidev/orderedjob"
)

type Metrics = orderedjob.Metrics
type NoopMetrics = orderedjob.NoopMetrics
type Logger = orderedjob.Logger
type SlogLogger = orderedjob.SlogLogger
type NoopLogger = orderedjob.NoopLogger

func NewSlogLogger(l *slog.Logger) *SlogLogger {
	return orderedjob.NewSlogLogger(l)
}

var LogJobAttrs = orderedjob.LogJobAttrs
