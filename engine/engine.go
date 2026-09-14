package engine

import (
	"context"

	orderedjob "github.com/semmidev/orderedjob"
)

type Engine = orderedjob.Engine
type Option = orderedjob.Option

var New = orderedjob.New
var WithConcurrency = orderedjob.WithConcurrency
var WithPollInterval = orderedjob.WithPollInterval
var WithLease = orderedjob.WithLease
var WithRetryPolicy = orderedjob.WithRetryPolicy
var WithMetrics = orderedjob.WithMetrics
var WithLogger = orderedjob.WithLogger
var WithWorkerID = orderedjob.WithWorkerID
var WithRecoveryInterval = orderedjob.WithRecoveryInterval
var WithNotify = orderedjob.WithNotify
var WithOrderingMode = orderedjob.WithOrderingMode

func RegisterTyped[T any](e *Engine, jobType string, fn func(ctx context.Context, job orderedjob.Job, payload T) error) {
	orderedjob.RegisterTyped(e, jobType, fn)
}

func RegisterTypedWithValidator[T any](e *Engine, jobType string, fn func(ctx context.Context, job orderedjob.Job, payload T) error, validator orderedjob.PayloadValidatorFunc[T]) {
	orderedjob.RegisterTypedWithValidator(e, jobType, fn, validator)
}
