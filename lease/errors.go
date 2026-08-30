package lease

import (
	"errors"
	"fmt"
)

var (
	ErrNotEligible        = errors.New("lease is not eligible")
	ErrConflict           = errors.New("lease update conflict")
	ErrUnknownOutcome     = errors.New("lease mutation outcome is unknown")
	ErrNotFound           = errors.New("lease object not found")
	ErrAlreadyHeld        = errors.New("lease is already held locally")
	ErrConcurrentMutation = errors.New("concurrent lease mutation")
	ErrInvalidLease       = errors.New("invalid lease")
	ErrLeaseExpired       = errors.New("lease expired")
	ErrLeaseRetired       = errors.New("lease retired")
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
	StoreOperationGet            StoreOperation = "get"
	StoreOperationCreateIfAbsent StoreOperation = "create_if_absent"
	StoreOperationCAS            StoreOperation = "compare_and_swap"
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

func (e *StoreError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("lease store %s: %s", e.Operation, e.Kind)
	}
	return fmt.Sprintf("lease store %s: %s: %v", e.Operation, e.Kind, e.Err)
}

func (e *StoreError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Is makes backend-neutral storage classifications available through
// errors.Is while retaining the adapter's original SDK error through Unwrap.
func (e *StoreError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrNotFound:
		return e.Kind == StoreErrorNotFound
	case ErrConflict:
		return e.Kind == StoreErrorPreconditionFailed || e.Kind == StoreErrorConflict
	case ErrUnknownOutcome:
		return e.Kind == StoreErrorOutcomeUnknown
	case ErrUnauthorized:
		return e.Kind == StoreErrorUnauthorized
	case ErrUnavailable:
		return e.Kind == StoreErrorTemporary
	case ErrProtocolViolation:
		return e.Kind == StoreErrorInvalidResponse
	default:
		return false
	}
}

// IsStoreError reports whether err contains a StoreError with the requested
// operation and kind. An empty operation matches any operation.
func IsStoreError(err error, operation StoreOperation, kind StoreErrorKind) bool {
	var storeErr *StoreError
	if !errors.As(err, &storeErr) {
		return false
	}
	return (operation == "" || storeErr.Operation == operation) && storeErr.Kind == kind
}

// StoreErrorKindOf extracts the backend-neutral storage classification.
func StoreErrorKindOf(err error) (StoreErrorKind, bool) {
	var storeErr *StoreError
	if !errors.As(err, &storeErr) {
		return "", false
	}
	return storeErr.Kind, true
}
