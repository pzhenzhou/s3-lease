// Package leaderelection defines a single-use leader-election recipe over the
// lease core. It follows the lifecycle shape of classic client-go election,
// while retaining this repository's stricter tracked-work and fencing rules.
package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/pkg/common"
	"github.com/pzhenzhou/s3-lease/recipes/internal/acquire"
	"github.com/pzhenzhou/s3-lease/recipes/internal/holder"
	"github.com/pzhenzhou/s3-lease/recipes/internal/schedule"
	"go.uber.org/zap"
)

var (
	ErrRunAlreadyUsed = errors.New("leader elector has already been used")
	ErrLeadershipLost = errors.New("leadership lost")
	ErrWorkNotStopped = errors.New("leader work did not stop")
)

// Config is construction-scoped and immutable for one Elector run.
type Config struct {
	Client          lease.Client
	RetryPeriod     time.Duration
	ObserveInterval time.Duration
	ShutdownTimeout time.Duration
	ReleaseOnCancel bool
	Callbacks       Callbacks
	Metrics         Metrics
	Logger          *zap.Logger
}

// Callbacks is run-scoped configuration, not a service interface. The start
// hook is required; the stop and observation hooks are optional.
type Callbacks struct {
	OnStartedLeading func(context.Context, uint64) error
	OnStoppedLeading func()
	OnLeaderObserved func(context.Context, lease.Observation)
}

// Elector is single-use. Create a new Elector to participate again.
type Elector struct {
	config Config
	mu     xsync.RBMutex
	used   bool
}

// New validates config and returns a single-use elector.
func New(config Config) (_ *Elector, err error) {
	logger := config.Logger
	if logger == nil {
		logger = common.Logger()
	}
	logger.Debug("leader elector construction started")
	defer func() {
		if err != nil {
			logger.Error("leader elector construction failed", zap.Error(err))
		}
	}()
	if nilInterface(config.Client) {
		return nil, fmt.Errorf("%w: lease client is required", lease.ErrInvalidConfig)
	}
	if config.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("%w: start-leading callback is required", lease.ErrInvalidConfig)
	}
	if config.RetryPeriod <= 0 || config.ObserveInterval <= 0 || config.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("%w: leader-election timing values must be positive", lease.ErrInvalidConfig)
	}
	timing := config.Client.Timing()
	if timing.RenewDeadline <= config.RetryPeriod ||
		timing.RenewDeadline-config.RetryPeriod <= config.RetryPeriod/5 {
		return nil, fmt.Errorf("%w: renew deadline must exceed 1.2 times retry period", lease.ErrInvalidConfig)
	}
	if nilInterface(config.Metrics) {
		config.Metrics = noopMetrics{}
	}
	config.Logger = logger
	logger.Info("leader elector constructed",
		zap.Duration("retry_period", config.RetryPeriod),
		zap.Duration("observe_interval", config.ObserveInterval),
		zap.Duration("shutdown_timeout", config.ShutdownTimeout),
		zap.Bool("release_on_cancel", config.ReleaseOnCancel))
	return &Elector{config: config}, nil
}

// Run waits for one confirmed acquisition, invokes the tracked leader work at
// most once, and never reacquires after that work starts or returns.
func (e *Elector) Run(ctx context.Context) (err error) {
	if e == nil {
		return fmt.Errorf("%w: nil leader elector", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if !e.enter() {
		return ErrRunAlreadyUsed
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	dispatcher := newObserverDispatcher(runCtx, e.config.Callbacks.OnLeaderObserved, e.config.Metrics)
	defer dispatcher.Stop()

	e.config.Logger.Info("leader election acquisition started")
	acquired, err := acquire.Run(runCtx, acquire.Config{
		Client:          e.config.Client,
		RetryPeriod:     e.config.RetryPeriod,
		ObserveInterval: e.config.ObserveInterval,
		OnObservation:   dispatcher.Submit,
	})
	if err != nil {
		e.logError("leader election acquisition failed", err)
		return err
	}
	if err := acquired.Check(); err != nil {
		lost := errors.Join(ErrLeadershipLost, err)
		e.logError("acquired leadership was invalid before work admission", lost)
		return lost
	}

	epochID := acquired.EpochID()
	e.config.Logger.Info("leader election acquisition confirmed", zap.Uint64("epoch_id", epochID))
	fatalErrors := e.startElectedObserver(runCtx, dispatcher)
	workAdmitted := false
	err = holder.Run(ctx, acquired, holder.Work(e.config.Callbacks.OnStartedLeading), holder.Policy{
		Client:              e.config.Client,
		RetryPeriod:         e.config.RetryPeriod,
		ShutdownTimeout:     e.config.ShutdownTimeout,
		ReleaseOnWorkReturn: true,
		ReleaseOnCancel:     e.config.ReleaseOnCancel,
		LossError:           ErrLeadershipLost,
		WorkNotStoppedError: ErrWorkNotStopped,
		FatalErrors:         fatalErrors,
		OnStarted: func() {
			workAdmitted = true
			e.config.Metrics.LeaderChanged(LeaderMetric{Held: true, EpochID: epochID})
			e.config.Logger.Info("leader work started", zap.Uint64("epoch_id", epochID))
		},
		OnStopped: func() {
			cancelRun()
			dispatcher.Stop()
			e.config.Metrics.LeaderChanged(LeaderMetric{Held: false, EpochID: epochID})
			e.config.Logger.Info("leader work stopping", zap.Uint64("epoch_id", epochID))
			if workAdmitted {
				if callback := e.config.Callbacks.OnStoppedLeading; callback != nil {
					go callback()
				}
			}
		},
		OnShutdown: func(duration time.Duration, timedOut bool) {
			e.config.Metrics.WorkShutdown(ShutdownMetric{Duration: duration, TimedOut: timedOut})
		},
	})
	cancelRun()
	dispatcher.Stop()
	if err != nil {
		e.logError("leader election lifecycle failed", err)
		return err
	}
	e.config.Logger.Info("leader election lifecycle completed", zap.Uint64("epoch_id", epochID))
	return nil
}

func (e *Elector) enter() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.used {
		return false
	}
	e.used = true
	return true
}

func (e *Elector) startElectedObserver(ctx context.Context, dispatcher *observerDispatcher) <-chan error {
	if e.config.Callbacks.OnLeaderObserved == nil {
		return nil
	}
	fatalErrors := make(chan error, 1)
	go func() {
		timer := time.NewTimer(schedule.Delay(e.config.ObserveInterval))
		defer stopElectionTimer(timer)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			observation, err := e.config.Client.Observe(ctx)
			if err == nil {
				dispatcher.Submit(observation)
			} else if ctx.Err() == nil && !acquire.ObservationRetryable(ctx, err) {
				select {
				case fatalErrors <- err:
				case <-ctx.Done():
				}
				return
			}
			resetElectionTimer(timer, e.config.ObserveInterval)
		}
	}()
	return fatalErrors
}

func (e *Elector) logError(message string, err error) {
	if err != nil {
		e.config.Logger.Error(message, zap.Error(err))
	}
}

func nilInterface(value any) bool {
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

func resetElectionTimer(timer *time.Timer, period time.Duration) {
	stopElectionTimer(timer)
	timer.Reset(schedule.Delay(period))
}

func stopElectionTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
