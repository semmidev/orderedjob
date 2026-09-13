package orderedjob

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound          = errors.New("orderedjob: job not found")
	ErrAlreadyExists     = errors.New("orderedjob: job already exists")
	ErrInvalidTransition = errors.New("orderedjob: invalid state transition")
	ErrLeaseConflict     = errors.New("orderedjob: lease conflict / fencing violation")
	ErrChainBlocked      = errors.New("orderedjob: chain blocked")
	ErrSequenceGap       = errors.New("orderedjob: sequence gap")
	ErrHandlerNotFound   = errors.New("orderedjob: handler not found")
	ErrNotClaimable      = errors.New("orderedjob: job not claimable")
	ErrDuplicateChainSeq = errors.New("orderedjob: duplicate chain_id, sequence")
)

// RetryableError marks a transient failure that should be retried.
type RetryableError struct{ Err error }

func (e RetryableError) Error() string { return fmt.Sprintf("retryable: %v", e.Err) }
func (e RetryableError) Unwrap() error { return e.Err }

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return RetryableError{Err: err}
}

// NonRetryableError marks a permanent failure.
type NonRetryableError struct{ Err error }

func (e NonRetryableError) Error() string { return fmt.Sprintf("non-retryable: %v", e.Err) }
func (e NonRetryableError) Unwrap() error { return e.Err }

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return NonRetryableError{Err: err}
}

func IsRetryable(err error) bool {
	var re RetryableError
	if errors.As(err, &re) {
		return true
	}
	var nre NonRetryableError
	if errors.As(err, &nre) {
		return false
	}
	// default: retryable for unknown errors, non-retryable is explicit.
	// We treat nil as not retryable check outside.
	return true
}

func IsNonRetryable(err error) bool {
	var nre NonRetryableError
	return errors.As(err, &nre)
}
