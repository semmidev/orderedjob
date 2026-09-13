package lifecycle

// States per proposal.
const (
	StateBlocked         = "BLOCKED"
	StatePending         = "PENDING"
	StateProcessing      = "PROCESSING"
	StateRetrying        = "RETRYING"
	StateCompleted       = "COMPLETED"
	StateFailed          = "FAILED"
	StateCancelRequested = "CANCEL_REQUESTED"
	StateCancelled       = "CANCELLED"
	StateTimedOut        = "TIMED_OUT"
	StateExpired         = "EXPIRED"
	StateDeadLettered    = "DEAD_LETTERED"
)

var terminalStates = map[string]bool{
	StateCompleted:    true,
	StateFailed:       true,
	StateCancelled:    true,
	StateExpired:      true,
	StateDeadLettered: true,
}

func IsTerminal(s string) bool { return terminalStates[s] }

// BlockNext returns true if this state must block the next sequence (C7).
func BlockNext(s string) bool {
	switch s {
	case StateCompleted:
		return false
	default:
		return true
	}
}

// CanExecute: only PENDING/RETRYING if eligible (eligibility handled in DB)
func CanBeClaimed(s string) bool {
	return s == StatePending || s == StateRetrying
}

// AllowedTransitions defines valid state edges.
var AllowedTransitions = map[string]map[string]bool{
	StateBlocked: {
		StatePending: true,
	},
	StatePending: {
		StateProcessing:      true,
		StateCancelRequested: true,
		StateExpired:         true,
	},
	StateProcessing: {
		StateCompleted:       true,
		StateRetrying:        true,
		StateFailed:          true,
		StateTimedOut:        true,
		StateCancelRequested: true,
	},
	StateRetrying: {
		StateProcessing:      true,
		StateCancelRequested: true,
		StateExpired:         true,
	},
	StateCancelRequested: {
		StateCancelled: true,
		StateCompleted: true, // if worker finished just before cancel ack
	},
	StateTimedOut: {
		StateRetrying: true,
		StateFailed:   true,
	},
	StateFailed: {
		StateDeadLettered: true,
	},
}

func IsValidTransition(from, to string) bool {
	if from == to {
		return true
	}
	if m, ok := AllowedTransitions[from]; ok {
		return m[to]
	}
	return false
}
