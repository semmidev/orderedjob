package orderedjob

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Ordering Mode Constants (Policy-Driven Execution Modes)
const (
	OrderingModeStrict                = "strict"
	OrderingModeSkipOnFailure         = "skip-on-failure"
	OrderingModeDeadLetterAndContinue = "dead-letter-and-continue"
)

// Job represents a unit of work bound to a chain.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	ChainID        string          `json:"chain_id"`
	Sequence       int64           `json:"sequence"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	TenantID       string          `json:"tenant_id,omitempty"`
	OrderingMode   string          `json:"ordering_mode,omitempty"`

	// runtime fields
	Status          string     `json:"status"`
	Attempt         int        `json:"attempt"`
	MaxAttempts     int        `json:"max_attempts"`
	AvailableAt     time.Time  `json:"available_at"`
	DeadlineAt      *time.Time `json:"deadline_at,omitempty"`
	WorkerID        string     `json:"worker_id,omitempty"`
	LeaseUntil      *time.Time `json:"lease_until,omitempty"`
	LeaseGeneration int        `json:"lease_generation"`
	LastError       string     `json:"last_error,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	FailedAt        *time.Time `json:"failed_at,omitempty"`

	TraceID string `json:"trace_id,omitempty"`
}

// EnqueueRequest is used by callers.
type EnqueueRequest struct {
	ChainID        string     `json:"chain_id"`
	Sequence       int64      `json:"sequence"`
	Type           string     `json:"job_type"`
	Payload        any        `json:"payload"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	TenantID       string     `json:"tenant_id,omitempty"`
	TraceID        string     `json:"trace_id,omitempty"`
	OrderingMode   string     `json:"ordering_mode,omitempty"`
	MaxAttempts    int        `json:"max_attempts,omitempty"`
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`
	AvailableAt    *time.Time `json:"available_at,omitempty"`
}

func (j Job) IsTerminal() bool {
	switch j.Status {
	case "COMPLETED", "FAILED", "CANCELLED", "EXPIRED", "DEAD_LETTERED":
		return true
	default:
		return false
	}
}
