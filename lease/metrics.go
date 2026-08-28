package lease

import "time"

type Operation string

const (
	OperationObserve Operation = "observe"
	OperationRequire Operation = "require"
	OperationRenew   Operation = "renew"
	OperationRelease Operation = "release"
)

type Outcome string

const (
	OutcomeSuccess     Outcome = "success"
	OutcomeNotEligible Outcome = "not_eligible"
	OutcomeConflict    Outcome = "conflict"
	OutcomeUnknown     Outcome = "unknown"
	OutcomeLost        Outcome = "lost"
	OutcomeInvalid     Outcome = "invalid"
	OutcomeUnavailable Outcome = "unavailable"
)

// OperationMetric is emitted once for one completed logical core call.
type OperationMetric struct {
	Key       Key
	ClientID  string
	Operation Operation
	Outcome   Outcome
	Duration  time.Duration
}

// GrantMetric is a point-in-time snapshot of process-local authority.
type GrantMetric struct {
	Key        Key
	ClientID   string
	Held       bool
	EpochID    uint64
	SequenceID uint64
	ValidUntil time.Time
}

// ObservationMetric is a point-in-time snapshot of locally read state.
type ObservationMetric struct {
	Key               Key
	ClientID          string
	EpochID           uint64
	SequenceID        uint64
	ObservationAge    time.Duration
	ConfirmedRenewAge time.Duration
}

// Metrics is a process-lifetime observability service consumed by a Lease.
// Event values are call-scoped and must not be mutated by the receiver.
type Metrics interface {
	OperationCompleted(OperationMetric)
	GrantChanged(GrantMetric)
	ObservationUpdated(ObservationMetric)
}
