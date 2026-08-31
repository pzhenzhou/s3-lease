// Package holder defines the private lifecycle framework shared by recipes.
package holder

import (
	"context"
	"errors"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/internal/schedule"
)

type Work func(context.Context, uint64) error

// Policy is acquisition-scoped configuration for one confirmed lease. It is
// discarded only after tracked work has joined or has timed out.
type Policy struct {
	Client              lease.Client
	RetryPeriod         time.Duration
	ShutdownTimeout     time.Duration
	ReleaseOnWorkReturn bool
	ReleaseOnCancel     bool
	LossError           error
	WorkNotStoppedError error
	OnStarted           func()
	OnStopped           func()
	OnShutdown          func(duration time.Duration, timedOut bool)
	// FatalErrors carries terminal errors from auxiliary holder activity, such
	// as elected-state observation. The channel must not be closed while Run is
	// active; a nil channel disables this input.
	FatalErrors <-chan error
}

type renewalResult struct {
	err       error
	resolving bool
}

type stopCause uint8

const (
	stopWorkReturned stopCause = iota + 1
	stopCallerCanceled
	stopLeaseLost
	stopFatal
)

// Run owns one acquired lease and one tracked work invocation. It returns only
// after work joins, except when it explicitly reports WorkNotStoppedError.
func Run(ctx context.Context, acquired *lease.Lease, work Work, policy Policy) error {
	if err := acquired.Check(); err != nil {
		return errors.Join(policy.LossError, err)
	}

	if policy.OnStarted != nil {
		policy.OnStarted()
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	workResult := make(chan error, 1)
	go func() { workResult <- work(workCtx, acquired.EpochID()) }()

	renewResults := make(chan renewalResult, 1)
	renewTimer := time.NewTimer(schedule.Delay(policy.RetryPeriod))
	defer stopTimer(renewTimer)
	var cancelRenew context.CancelFunc
	var renewing bool
	var renewalPending bool
	var releaseSuppressed bool

	var cause stopCause
	var primary error
	var returnedWork error
	workJoined := false

running:
	for {
		select {
		case returnedWork = <-workResult:
			workJoined = true
			if ctx.Err() != nil {
				cause = stopCallerCanceled
				primary = ctx.Err()
			} else {
				cause = stopWorkReturned
				primary = returnedWork
			}
			break running
		case <-ctx.Done():
			cause = stopCallerCanceled
			primary = ctx.Err()
			break running
		case <-acquired.Done():
			cause = stopLeaseLost
			primary = errors.Join(policy.LossError, acquired.Check())
			break running
		case fatalErr := <-policy.FatalErrors:
			if fatalErr == nil {
				continue
			}
			cause = stopFatal
			primary = fatalErr
			break running
		case <-renewTimer.C:
			if renewing {
				continue
			}
			renewing = true
			resolving := renewalPending
			var renewCtx context.Context
			renewCtx, cancelRenew = context.WithCancel(ctx)
			go func() {
				renewResults <- renewalResult{err: policy.Client.Renew(renewCtx, acquired), resolving: resolving}
			}()
		case result := <-renewResults:
			renewing = false
			cancelRenew()
			cancelRenew = nil
			switch {
			case result.err == nil:
				renewalPending = false
				if result.resolving {
					// Reconciliation confirms the original proposal and its
					// original first-send deadline. Renew again immediately so
					// response loss cannot consume most of the remaining margin.
					resetTimer(renewTimer, 0)
				} else {
					resetTimer(renewTimer, policy.RetryPeriod)
				}
			case ctx.Err() != nil:
				cause = stopCallerCanceled
				primary = ctx.Err()
				renewalPending, releaseSuppressed = stoppedRenewalState(result)
				break running
			case errors.Is(result.err, lease.ErrUnknownOutcome):
				renewalPending = true
				if result.resolving {
					// Repeated ambiguity while reconciling must not become a
					// request hot loop. Preserve the proposal and retry normally.
					resetTimer(renewTimer, policy.RetryPeriod)
				} else {
					// Resolve the first ambiguous result promptly. This does not
					// allocate a new renewal or change its conditional body.
					resetTimer(renewTimer, 0)
				}
			case retryable(result.err):
				// A transient reconciliation failure leaves the exact frozen
				// proposal unresolved. Keep reconciling it until authority ends;
				// never create a fresh renewal while its outcome is unknown.
				renewalPending = result.resolving
				resetTimer(renewTimer, policy.RetryPeriod)
			case result.resolving:
				cause = stopFatal
				primary = result.err
				break running
			case isLeaseLoss(result.err):
				cause = stopLeaseLost
				primary = errors.Join(policy.LossError, result.err)
				break running
			default:
				cause = stopFatal
				primary = result.err
				break running
			}
		}
	}

	stopDeadline := acquired.ValidUntil()
	stopTimer(renewTimer)
	cancelWork()
	if cause != stopWorkReturned && cancelRenew != nil {
		cancelRenew()
	}
	if policy.OnStopped != nil {
		policy.OnStopped()
	}

	shutdownStarted := time.Now()
	if !workJoined {
		shutdownTimer := time.NewTimer(policy.ShutdownTimeout)
		select {
		case returnedWork = <-workResult:
			workJoined = true
			stopTimer(shutdownTimer)
		case <-shutdownTimer.C:
			if policy.OnShutdown != nil {
				policy.OnShutdown(time.Since(shutdownStarted), true)
			}
			if cancelRenew != nil {
				cancelRenew()
			}
			return errors.Join(primary, policy.WorkNotStoppedError)
		}
	}
	if policy.OnShutdown != nil {
		policy.OnShutdown(time.Since(shutdownStarted), false)
	}
	if returnedWork != nil && cause != stopWorkReturned &&
		(ctx.Err() == nil || !errors.Is(returnedWork, ctx.Err())) {
		primary = errors.Join(primary, returnedWork)
	}

	var renewalErr error
	if renewing {
		result, ok := waitRenewal(stopDeadline, renewResults)
		if !ok {
			if cancelRenew != nil {
				cancelRenew()
			}
			renewalErr = lease.ErrLeaseExpired
		} else {
			renewing = false
			if cancelRenew != nil {
				cancelRenew()
				cancelRenew = nil
			}
			renewalErr = result.err
			renewalPending, releaseSuppressed = stoppedRenewalState(result)
		}
	}

	releaseWanted := cause == stopWorkReturned && policy.ReleaseOnWorkReturn ||
		cause == stopCallerCanceled && policy.ReleaseOnCancel
	if !releaseWanted || cause == stopLeaseLost || cause == stopFatal || releaseSuppressed {
		return errors.Join(primary, renewalErr)
	}

	cleanupCtx, cancelCleanup, ok := cleanupContext(ctx, stopDeadline)
	if !ok {
		return errors.Join(primary, renewalErr, policy.LossError, lease.ErrLeaseExpired)
	}
	defer cancelCleanup()
	if renewalPending {
		if err := policy.Client.Renew(cleanupCtx, acquired); err != nil {
			return errors.Join(primary, renewalErr, err)
		}
		// Exact reconciliation converted the formerly unknown outcome into a
		// confirmed renewal, so it is no longer a terminal cleanup error.
		renewalErr = nil
	}
	if err := acquired.Check(); err != nil {
		return errors.Join(primary, renewalErr, policy.LossError, err)
	}
	releaseErr := policy.Client.Release(cleanupCtx, acquired)
	// Once Release has frozen an uncertain proposal, subsequent Release calls
	// can only reconcile that exact proposal in the core. Bound reconciliation
	// by both the original authority deadline and a small attempt count.
	for attempt := 1; attempt < 3 && errors.Is(releaseErr, lease.ErrUnknownOutcome) && cleanupCtx.Err() == nil; attempt++ {
		releaseErr = policy.Client.Release(cleanupCtx, acquired)
	}
	return errors.Join(primary, renewalErr, releaseErr)
}

// stoppedRenewalState reports whether shutdown may safely issue the one
// permitted exact reconciliation. A non-unknown error from a reconciliation
// is deliberately not treated as pending: the core may have cleared the old
// proposal, so another Renew could create a fresh proposal while draining.
func stoppedRenewalState(result renewalResult) (pending, suppressRelease bool) {
	switch {
	case result.err == nil:
		return false, false
	case errors.Is(result.err, lease.ErrUnknownOutcome):
		return true, false
	case result.resolving:
		return false, true
	default:
		return false, false
	}
}

func retryable(err error) bool {
	return errors.Is(err, lease.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func isLeaseLoss(err error) bool {
	return errors.Is(err, lease.ErrLeaseExpired) || errors.Is(err, lease.ErrLeaseRetired) ||
		errors.Is(err, lease.ErrOwnershipLost) || errors.Is(err, lease.ErrInvalidLease)
}

func cleanupContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc, bool) {
	if deadline.IsZero() || !time.Now().Before(deadline) {
		return nil, nil, false
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(parent), deadline)
	return ctx, cancel, true
}

func waitRenewal(deadline time.Time, results <-chan renewalResult) (renewalResult, bool) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return renewalResult{}, false
	}
	timer := time.NewTimer(remaining)
	defer stopTimer(timer)
	select {
	case result := <-results:
		return result, true
	case <-timer.C:
		return renewalResult{}, false
	}
}

func resetTimer(timer *time.Timer, period time.Duration) {
	stopTimer(timer)
	timer.Reset(schedule.Delay(period))
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
