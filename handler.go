package orderedjob

import (
	"context"
	"encoding/json"
	"fmt"
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

// RegisterTyped registers a type-safe handler function that automatically unmarshals JSON payloads.
func RegisterTyped[T any](e *Engine, jobType string, fn func(ctx context.Context, job Job, payload T) error) {
	e.RegisterFunc(jobType, func(ctx context.Context, job Job) error {
		var p T
		if len(job.Payload) > 0 && string(job.Payload) != "null" {
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return NonRetryable(fmt.Errorf("unmarshal payload: %w", err))
			}
		}
		return fn(ctx, job, p)
	})
}
