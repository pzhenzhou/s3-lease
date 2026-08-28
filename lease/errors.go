package lease

import (
	"errors"
)

var (
	ErrNotEligible        = errors.New("lease is not eligible")
	ErrConflict           = errors.New("lease update conflict")
	ErrUnknownOutcome     = errors.New("lease mutation outcome is unknown")
	ErrNotFound           = errors.New("lease object not found")
	ErrAlreadyHeld        = errors.New("lease is already held locally")
	ErrConcurrentMutation = errors.New("concurrent lease mutation")
	ErrInvalidHandle      = errors.New("invalid lease handle")
	ErrHandleExpired      = errors.New("lease handle expired")
	ErrHandleRetired      = errors.New("lease handle retired")
	ErrOwnershipLost      = errors.New("lease ownership lost")
	ErrLeaseDisappeared   = errors.New("known lease object disappeared")
	ErrProtocolViolation  = errors.New("lease protocol violation")
	ErrCounterOverflow    = errors.New("lease counter overflow")
	ErrUnauthorized       = errors.New("lease storage unauthorized")
	ErrUnavailable        = errors.New("lease storage unavailable")
	ErrInvalidConfig      = errors.New("invalid lease configuration")
)

// StoreOperation identifies one call-scoped storage action.
type StoreOperation string

const (
	StoreOperationGet    StoreOperation = "get"
	StoreOperationCreate StoreOperation = "create"
	StoreOperationCAS    StoreOperation = "compare_and_swap"
)

// StoreErrorKind is the backend-neutral classification consumed by the core.
type StoreErrorKind string

const (
	StoreErrorNotFound           StoreErrorKind = "not_found"
	StoreErrorPreconditionFailed StoreErrorKind = "precondition_failed"
	StoreErrorConflict           StoreErrorKind = "conflict"
	StoreErrorUnauthorized       StoreErrorKind = "unauthorized"
	StoreErrorTemporary          StoreErrorKind = "temporary"
	StoreErrorOutcomeUnknown     StoreErrorKind = "outcome_unknown"
	StoreErrorInvalidResponse    StoreErrorKind = "invalid_response"
)

// StoreError is an operation-scoped classification value. The eventual store
// adapter wraps its SDK cause before crossing into the lease core.
type StoreError struct {
	Operation StoreOperation
	Kind      StoreErrorKind
	Err       error
}
