package mutex

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/internal/schedule"
	"go.uber.org/zap"
)

type manualRenewalResult struct {
	pending          bool
	suppressRelease  bool
	authorityWasLost bool
	err              error
}

// Lock is one acquisition returned by TryLock. It cannot be transferred to a
// different Mutex or used to release a later acquisition.
type Lock struct {
	origin   *Mutex
	acquired *lease.Lease

	mu               xsync.RBMutex
	cancelRenew      context.CancelFunc
	renewDone        chan manualRenewalResult
	stopped          bool
	pending          bool
	suppressRelease  bool
	authorityWasLost bool
	terminal         error
	done             chan struct{}
}

// TryLock performs exactly one core acquisition attempt. It never enters the
// blocking acquisition loop used by WithLock.
func (m *Mutex) TryLock(ctx context.Context) (*Lock, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil mutex", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if !m.enter() {
		return nil, ErrRecipeBusy
	}
	acquired, err := m.config.Client.Require(ctx)
	if err != nil {
		m.clearBusy()
		m.logError(err)
		return nil, err
	}
	if err := acquired.Check(); err != nil {
		m.clearBusy()
		return nil, errors.Join(ErrLeaseLost, err)
	}

	renewCtx, cancelRenew := context.WithCancel(context.WithoutCancel(ctx))
	held := &Lock{
		origin:      m,
		acquired:    acquired,
		cancelRenew: cancelRenew,
		renewDone:   make(chan manualRenewalResult, 1),
		done:        make(chan struct{}),
	}
	m.mu.Lock()
	m.manual = held
	m.mu.Unlock()
	m.config.Metrics.LockChanged(true, acquired.EpochID())
	m.config.Logger.Info("mutex try-lock acquired", zap.Uint64("epoch_id", acquired.EpochID()))
	go held.maintain(renewCtx)
	return held, nil
}

// Release stops automatic renewal and conditionally releases the exact
// acquisition represented by held. The caller must stop protected work first.
func (m *Mutex) Release(ctx context.Context, held *Lock) error {
	if m == nil {
		return fmt.Errorf("%w: nil mutex", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if held == nil || held.origin != m {
		return ErrLockNotHeld
	}
	if err := m.beginRelease(held); err != nil {
		return err
	}

	stopDeadline := held.acquired.ValidUntil()
	result, waitErr := held.stopRenewal(ctx)
	if waitErr != nil {
		// Renewal has already been canceled and cannot safely be restarted.
		// Retire the public handle now, then retain local ownership until the
		// underlying grant reaches its independently enforced deadline.
		m.abandonManual(held, waitErr)
		return waitErr
	}
	pending := result.pending
	renewalErr := result.err
	if result.suppressRelease {
		return renewalErr
	}
	cleanupCtx, cancelCleanup, ok := manualCleanupContext(ctx, stopDeadline)
	if !ok {
		lost := errors.Join(ErrLeaseLost, held.acquired.Check(), lease.ErrLeaseExpired)
		m.finishRelease(held, lost)
		return errors.Join(renewalErr, lost)
	}
	defer cancelCleanup()

	if pending {
		if err := m.config.Client.Renew(cleanupCtx, held.acquired); err != nil {
			cleanupErr := errors.Join(renewalErr, err)
			m.abandonManual(held, cleanupErr)
			return cleanupErr
		}
		held.clearPending()
		renewalErr = nil
	}
	if err := held.acquired.Check(); err != nil {
		lost := errors.Join(ErrLeaseLost, err)
		m.finishRelease(held, lost)
		return errors.Join(renewalErr, lost)
	}

	releaseErr := m.config.Client.Release(cleanupCtx, held.acquired)
	if releaseErr != nil && held.acquired.Check() == nil {
		cleanupErr := errors.Join(renewalErr, releaseErr)
		m.abandonManual(held, cleanupErr)
		return cleanupErr
	}
	m.finishRelease(held, held.acquired.Check())
	return errors.Join(renewalErr, releaseErr)
}

// EpochID returns the fencing epoch for this manual acquisition.
func (l *Lock) EpochID() uint64 {
	if l == nil || l.acquired == nil {
		return 0
	}
	return l.acquired.EpochID()
}

// ValidUntil returns the core's current local authority deadline.
func (l *Lock) ValidUntil() time.Time {
	if l == nil || l.acquired == nil {
		return time.Time{}
	}
	return l.acquired.ValidUntil()
}

// Done closes when renewal stops because authority was lost or Release retires
// this acquisition.
func (l *Lock) Done() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.done
}

// Check synchronously verifies that the manual acquisition remains usable.
func (l *Lock) Check() error {
	if l == nil || l.acquired == nil {
		return lease.ErrInvalidLease
	}
	token := l.mu.RLock()
	terminal := l.terminal
	l.mu.RUnlock(token)
	if terminal != nil {
		return terminal
	}
	return l.acquired.Check()
}

func (l *Lock) maintain(ctx context.Context) {
	result := l.renewLoop(ctx)
	l.mu.Lock()
	l.stopped = true
	l.pending = result.pending
	l.suppressRelease = result.suppressRelease
	l.authorityWasLost = result.authorityWasLost
	l.mu.Unlock()
	if result.err != nil {
		if result.authorityWasLost {
			l.origin.clearManual(l)
			l.retire(result.err)
		} else {
			l.origin.abandonManual(l, result.err)
		}
	}
	l.renewDone <- result
}

func (l *Lock) renewLoop(ctx context.Context) manualRenewalResult {
	timer := time.NewTimer(schedule.Delay(l.origin.config.RetryPeriod))
	defer stopTimer(timer)
	pending := false
	for {
		select {
		case <-ctx.Done():
			return manualRenewalResult{pending: pending}
		case <-l.acquired.Done():
			return manualRenewalResult{
				authorityWasLost: true,
				err:              errors.Join(ErrLeaseLost, l.acquired.Check()),
			}
		case <-timer.C:
		}

		resolving := pending
		err := l.origin.config.Client.Renew(ctx, l.acquired)
		if ctx.Err() != nil {
			return stoppedManualRenewalState(resolving, err)
		}
		switch {
		case err == nil:
			pending = false
		case errors.Is(err, lease.ErrUnknownOutcome):
			pending = true
		case isManualLeaseLoss(err):
			return manualRenewalResult{
				authorityWasLost: true,
				err:              errors.Join(ErrLeaseLost, err),
			}
		case resolving:
			return manualRenewalResult{suppressRelease: true, err: err}
		case manualRenewalRetryable(err):
			pending = false
		default:
			return manualRenewalResult{pending: false, err: err}
		}
		resetTimer(timer, l.origin.config.RetryPeriod)
	}
}

func (l *Lock) stopRenewal(ctx context.Context) (manualRenewalResult, error) {
	l.mu.Lock()
	stopped := l.stopped
	pending := l.pending
	suppressRelease := l.suppressRelease
	authorityWasLost := l.authorityWasLost
	terminal := l.terminal
	cancel := l.cancelRenew
	l.mu.Unlock()
	if stopped {
		return manualRenewalResult{
			pending:          pending,
			suppressRelease:  suppressRelease,
			authorityWasLost: authorityWasLost,
			err:              terminal,
		}, nil
	}
	cancel()
	select {
	case result := <-l.renewDone:
		return result, nil
	case <-ctx.Done():
		return manualRenewalResult{}, ctx.Err()
	}
}

func (l *Lock) clearPending() {
	l.mu.Lock()
	l.pending = false
	l.mu.Unlock()
}

func (l *Lock) retire(err error) {
	l.mu.Lock()
	if l.terminal == nil {
		l.terminal = err
	}
	select {
	case <-l.done:
		l.mu.Unlock()
		return
	default:
		close(l.done)
	}
	l.mu.Unlock()
	l.origin.config.Metrics.LockChanged(false, l.EpochID())
}

func (m *Mutex) beginRelease(held *Lock) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.busy || m.manual != held {
		return ErrLockNotHeld
	}
	if m.releasing {
		return ErrRecipeBusy
	}
	m.releasing = true
	return nil
}

func (m *Mutex) finishRelease(held *Lock, terminal error) {
	held.retire(terminal)
	m.clearManual(held)
	m.config.Logger.Info("mutex manual lock released", zap.Uint64("epoch_id", held.EpochID()), zap.Error(terminal))
}

func (m *Mutex) abandonManual(held *Lock, cause error) {
	held.retire(cause)
	m.mu.Lock()
	if m.manual == held {
		// Keep the Mutex unavailable until the core's existing authority retires.
		// Retrying an uncertain renewal cleanup could accidentally create a new
		// renewal instead of reconciling the old one.
		m.releasing = true
	}
	m.mu.Unlock()
	go func() {
		<-held.acquired.Done()
		m.mu.Lock()
		if m.manual == held {
			m.manual = nil
			m.busy = false
			m.releasing = false
		}
		m.mu.Unlock()
	}()
}

func (m *Mutex) clearManual(held *Lock) {
	m.mu.Lock()
	if m.manual == held {
		m.manual = nil
		m.busy = false
		m.releasing = false
	}
	m.mu.Unlock()
}

func stoppedManualRenewalState(resolving bool, err error) manualRenewalResult {
	switch {
	case err == nil:
		return manualRenewalResult{}
	case errors.Is(err, lease.ErrUnknownOutcome):
		return manualRenewalResult{pending: true}
	case isManualLeaseLoss(err):
		return manualRenewalResult{
			authorityWasLost: true,
			err:              errors.Join(ErrLeaseLost, err),
		}
	case resolving:
		return manualRenewalResult{suppressRelease: true, err: err}
	default:
		return manualRenewalResult{}
	}
}

func manualRenewalRetryable(err error) bool {
	return errors.Is(err, lease.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func isManualLeaseLoss(err error) bool {
	return errors.Is(err, lease.ErrLeaseExpired) || errors.Is(err, lease.ErrLeaseRetired) ||
		errors.Is(err, lease.ErrOwnershipLost) || errors.Is(err, lease.ErrInvalidLease)
}

func manualCleanupContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc, bool) {
	if deadline.IsZero() || !time.Now().Before(deadline) {
		return nil, nil, false
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel, true
}
