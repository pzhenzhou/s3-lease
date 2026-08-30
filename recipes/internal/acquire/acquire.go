// Package acquire owns the retry and observation loop shared by blocking
// coordination recipes.
package acquire

import (
	"context"
	"errors"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/internal/schedule"
)

// Config describes one blocking acquisition. OnObservation must return
// quickly; recipes that invoke application callbacks should enqueue snapshots
// rather than running callbacks in this loop.
type Config struct {
	Client          lease.Client
	RetryPeriod     time.Duration
	ObserveInterval time.Duration
	OnObservation   func(lease.Observation)
}

// Run performs one immediate acquisition attempt, then interleaves jittered
// retries and observations until it acquires or encounters a terminal error.
// A Require attempt resets the observation timer because Require already read
// current state while deciding eligibility.
func Run(ctx context.Context, config Config) (*lease.Lease, error) {
	acquired, err := config.Client.Require(ctx)
	if err == nil {
		return acquired, nil
	}
	if !Retryable(ctx, err) {
		return nil, err
	}

	retryTimer := time.NewTimer(schedule.Delay(config.RetryPeriod))
	observeTimer := time.NewTimer(schedule.Delay(config.ObserveInterval))
	defer stopTimer(retryTimer)
	defer stopTimer(observeTimer)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-retryTimer.C:
			acquired, err = config.Client.Require(ctx)
			if err == nil {
				return acquired, nil
			}
			if !Retryable(ctx, err) {
				return nil, err
			}
			resetTimer(retryTimer, config.RetryPeriod)
			resetTimer(observeTimer, config.ObserveInterval)
		case <-observeTimer.C:
			observation, observeErr := config.Client.Observe(ctx)
			if observeErr == nil {
				if config.OnObservation != nil {
					config.OnObservation(observation)
				}
			} else if !ObservationRetryable(ctx, observeErr) {
				return nil, observeErr
			}
			resetTimer(observeTimer, config.ObserveInterval)
		}
	}
}

// Retryable reports whether an acquisition failure may be retried without
// hiding a protocol or authorization failure.
func Retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	return errors.Is(err, lease.ErrNotEligible) || errors.Is(err, lease.ErrConflict) ||
		errors.Is(err, lease.ErrUnknownOutcome) || errors.Is(err, lease.ErrUnavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

// ObservationRetryable reports errors that do not make a waiting participant
// terminal. ErrNotFound is allowed only before the core has observed an
// object; after that point the core converts disappearance into a terminal
// ErrLeaseDisappeared error.
func ObservationRetryable(ctx context.Context, err error) bool {
	return Retryable(ctx, err) || errors.Is(err, lease.ErrNotFound)
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
