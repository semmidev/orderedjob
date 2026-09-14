package orderedjob

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
)

type Handler interface {
	Handle(ctx context.Context, job Job) error
}

type HandlerFunc func(ctx context.Context, job Job) error

func (f HandlerFunc) Handle(ctx context.Context, job Job) error { return f(ctx, job) }

// Registry holds job_type -> handler
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

func (r *Registry) MustGet(jobType string) (Handler, error) {
	h, ok := r.Get(jobType)
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return h, nil
}

// JobTypes returns all registered job type names in sorted order.
func (r *Registry) JobTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.handlers))
}

// Validator is an interface that payload structs can implement to perform self-validation.
type Validator interface {
	Validate() error
}

// ValidatorCtx is an interface that payload structs can implement to perform self-validation with context.
type ValidatorCtx interface {
	ValidateCtx(ctx context.Context) error
}

// PayloadValidatorFunc is a custom validation function type for a payload of type T.
type PayloadValidatorFunc[T any] func(ctx context.Context, payload T) error

// RegisterTyped registers a type-safe handler function that automatically unmarshals JSON payloads.
// If payload type T implements Validator or ValidatorCtx interface, validation is executed automatically before calling fn.
func RegisterTyped[T any](e *Engine, jobType string, fn func(ctx context.Context, job Job, payload T) error) {
	RegisterTypedWithValidator(e, jobType, fn, nil)
}

// RegisterTypedWithValidator registers a type-safe handler function with optional custom payload validation logic.
// Validation occurs after JSON unmarshaling and self-validation. If validation returns an error, execution fails
// immediately with a NonRetryable error wrapping ErrValidationFailed.
func RegisterTypedWithValidator[T any](
	e *Engine,
	jobType string,
	fn func(ctx context.Context, job Job, payload T) error,
	validator PayloadValidatorFunc[T],
) {
	e.RegisterFunc(jobType, func(ctx context.Context, job Job) error {
		var p T
		if len(job.Payload) > 0 && string(job.Payload) != "null" {
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return NonRetryable(fmt.Errorf("unmarshal payload: %w", err))
			}
		}

		// 1. Self-validation via Validator interface
		if v, ok := any(p).(Validator); ok {
			if err := v.Validate(); err != nil {
				return NonRetryable(fmt.Errorf("%w: %w", ErrValidationFailed, err))
			}
		} else if v, ok := any(&p).(Validator); ok {
			if err := v.Validate(); err != nil {
				return NonRetryable(fmt.Errorf("%w: %w", ErrValidationFailed, err))
			}
		}

		// 2. Self-validation via ValidatorCtx interface
		if v, ok := any(p).(ValidatorCtx); ok {
			if err := v.ValidateCtx(ctx); err != nil {
				return NonRetryable(fmt.Errorf("%w: %w", ErrValidationFailed, err))
			}
		} else if v, ok := any(&p).(ValidatorCtx); ok {
			if err := v.ValidateCtx(ctx); err != nil {
				return NonRetryable(fmt.Errorf("%w: %w", ErrValidationFailed, err))
			}
		}

		// 3. Custom validator function if provided
		if validator != nil {
			if err := validator(ctx, p); err != nil {
				return NonRetryable(fmt.Errorf("%w: %w", ErrValidationFailed, err))
			}
		}

		return fn(ctx, job, p)
	})
}
