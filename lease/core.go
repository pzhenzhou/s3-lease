package lease

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/pkg/common"
	"go.uber.org/zap"
)

type proposalKind uint8

const (
	proposalAcquire proposalKind = iota + 1
	proposalRenew
	proposalRelease
)

type core struct {
	config Config
	logger *zap.Logger

	// mutationMu prevents Require, Renew, and Release from freezing competing
	// proposals from the same ETag or replacing the one unresolved proposal.
	// TryLock rejects overlap rather than hiding it behind a local queue.
	mutationMu xsync.RBMutex
	// stateMu serializes caller operations, expiry callbacks, observations, and
	// store responses that read or replace local authority. In particular, it
	// prevents an expiry callback and a late renewal response from both changing
	// an acquired lease's deadline and thereby reviving expired authority.
	stateMu  xsync.RBMutex
	observed *observedState
	active   *Lease
	pending  *proposal
	terminal error
}

type observedState struct {
	record         Record
	body           []byte
	version        Version
	unchangedSince time.Time
	lastReadAt     time.Time
}

type proposal struct {
	kind          proposalKind
	acquired      *Lease
	record        Record
	body          []byte
	expected      Version
	firstSendAt   time.Time
	authorityEnds time.Time
}

// Lease is acquisition-scoped local authority. It cannot be serialized,
// transferred to another Client, or revived after retirement.
type Lease struct {
	origin     *core
	leaseUID   string
	clientID   string
	epochID    uint64
	sequenceID uint64
	validUntil time.Time
	done       chan struct{}
	retired    error
	stopTimer  func() bool
}

type realClock struct{}

func (k proposalKind) String() string {
	switch k {
	case proposalAcquire:
		return "acquire"
	case proposalRenew:
		return "renew"
	case proposalRelease:
		return "release"
	default:
		return "unknown"
	}
}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(delay time.Duration, fn func()) func() bool {
	timer := time.AfterFunc(delay, fn)
	return timer.Stop
}

// New validates and copies config, returning a reusable lease client
// bound to exactly one backend, bucket, key, and stable client identity.
func New(config Config) (_ Client, err error) {
	logger := config.Logger
	if logger == nil {
		logger = common.Logger()
	}
	logger.Debug("lease client construction started",
		zap.String("bucket", config.Key.Bucket),
		zap.String("object_key", config.Key.ObjectKey),
		zap.String("client_id", config.ClientID))
	defer func() {
		if err != nil {
			logger.Error("lease client construction failed", zap.Error(err))
		}
	}()
	if err = validateConfig(config); err != nil {
		return nil, err
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	config.Logger = logger
	logger.Info("lease client constructed",
		zap.String("bucket", config.Key.Bucket),
		zap.String("object_key", config.Key.ObjectKey),
		zap.String("client_id", config.ClientID),
		zap.Duration("lease_duration", config.LeaseDuration),
		zap.Duration("renew_deadline", config.RenewDeadline))
	return &core{config: config, logger: logger}, nil
}

func validateConfig(config Config) error {
	if isNilInterface(config.Store) {
		return fmt.Errorf("%w: store is required", ErrInvalidConfig)
	}
	if config.Clock != nil && isNilInterface(config.Clock) {
		return fmt.Errorf("%w: clock must not be a typed nil", ErrInvalidConfig)
	}
	if strings.TrimSpace(config.Key.Bucket) == "" {
		return fmt.Errorf("%w: bucket is required", ErrInvalidConfig)
	}
	if config.Key.ObjectKey == "" {
		return fmt.Errorf("%w: object key is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(config.ClientID) == "" {
		return fmt.Errorf("%w: client ID is required", ErrInvalidConfig)
	}
	if config.LeaseDuration <= 0 || config.RenewDeadline <= 0 || config.RequestTimeout <= 0 {
		return fmt.Errorf("%w: timing values must be positive", ErrInvalidConfig)
	}
	if config.LeaseDuration%time.Second != 0 {
		return fmt.Errorf("%w: lease duration must be an integral number of seconds", ErrInvalidConfig)
	}
	if config.LeaseDuration <= config.RenewDeadline {
		return fmt.Errorf("%w: lease duration must exceed renew deadline", ErrInvalidConfig)
	}
	if config.RequestTimeout >= config.RenewDeadline {
		return fmt.Errorf("%w: request timeout must be less than renew deadline", ErrInvalidConfig)
	}
	return nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Timing returns immutable construction-time timing metadata without I/O.
func (c *core) Timing() Timing {
	return Timing{
		LeaseDuration:  c.config.LeaseDuration,
		RenewDeadline:  c.config.RenewDeadline,
		RequestTimeout: c.config.RequestTimeout,
	}
}

// EpochID returns this lease's fencing epoch.
func (l *Lease) EpochID() uint64 {
	if l == nil {
		return 0
	}
	return l.epochID
}

// ValidUntil returns the local monotonic authority deadline.
func (l *Lease) ValidUntil() time.Time {
	if l == nil || l.origin == nil {
		return time.Time{}
	}
	token := l.origin.stateMu.RLock()
	deadline := l.validUntil
	l.origin.stateMu.RUnlock(token)
	return deadline
}

// Done closes exactly once when this lease expires or is retired.
func (l *Lease) Done() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.done
}

// Check synchronously verifies local authority; it never performs storage I/O.
func (l *Lease) Check() (err error) {
	if l == nil || l.origin == nil {
		return ErrInvalidLease
	}
	c := l.origin
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if l.retired != nil {
		err = l.retired
	} else if c.active != l {
		err = ErrInvalidLease
	} else if !c.config.Clock.Now().Before(l.validUntil) {
		c.retireLeaseLocked(l, ErrLeaseExpired)
		err = ErrLeaseExpired
	}
	if err != nil {
		c.logger.Error("lease check failed", zap.Uint64("epoch_id", l.epochID), zap.Error(err))
	}
	return err
}

func (c *core) retireLeaseLocked(acquired *Lease, cause error) {
	if acquired == nil || acquired.retired != nil {
		return
	}
	acquired.retired = cause
	if acquired.stopTimer != nil {
		acquired.stopTimer()
		acquired.stopTimer = nil
	}
	close(acquired.done)
	if c.active == acquired {
		c.active = nil
	}
	c.logger.Info("lease retired",
		zap.Uint64("epoch_id", acquired.epochID),
		zap.Uint64("sequence_id", acquired.sequenceID),
		zap.Error(cause))
}

func (c *core) installTimerLocked(acquired *Lease) {
	if acquired.stopTimer != nil {
		acquired.stopTimer()
	}
	delay := acquired.validUntil.Sub(c.config.Clock.Now())
	acquired.stopTimer = c.config.Clock.AfterFunc(delay, func() {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		if c.active == acquired && acquired.retired == nil && !c.config.Clock.Now().Before(acquired.validUntil) {
			c.retireLeaseLocked(acquired, ErrLeaseExpired)
		}
	})
}

func (c *core) requestContext(parent context.Context, phaseEnd time.Time) (context.Context, context.CancelFunc, error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	budget := c.config.RequestTimeout
	if !phaseEnd.IsZero() {
		remaining := phaseEnd.Sub(c.config.Clock.Now())
		if remaining <= 0 {
			return nil, nil, context.DeadlineExceeded
		}
		if remaining < budget {
			budget = remaining
		}
	}
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil, context.DeadlineExceeded
		}
		if remaining < budget {
			budget = remaining
		}
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	return ctx, cancel, nil
}

func (c *core) setTerminalLocked(err error) error {
	if c.terminal == nil {
		c.terminal = err
		if c.active != nil {
			c.retireLeaseLocked(c.active, ErrOwnershipLost)
		}
	}
	return c.terminal
}

func (c *core) logMethodError(method string, err error) {
	if err != nil {
		c.logger.Error("lease method failed", zap.String("method", method), zap.Error(err))
	}
}

func isDefinitiveConflict(err error) bool {
	return IsStoreError(err, "", StoreErrorPreconditionFailed) ||
		IsStoreError(err, "", StoreErrorConflict) || errors.Is(err, ErrConflict)
}
