// Package mutex defines scoped and manual distributed-lock APIs over the lease
// core.
package mutex

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
	"go.uber.org/zap"
)

var (
	ErrRecipeBusy     = errors.New("mutex recipe is busy")
	ErrLockNotHeld    = errors.New("mutex lock is not held")
	ErrLeaseLost      = errors.New("mutex lease lost")
	ErrWorkNotStopped = errors.New("mutex work did not stop")
)

// Config is construction-scoped and immutable for the lifetime of a Mutex.
type Config struct {
	Client          lease.Client
	RetryPeriod     time.Duration
	ObserveInterval time.Duration
	ShutdownTimeout time.Duration
	ReleaseOnCancel bool
	Metrics         Metrics
	Logger          *zap.Logger
}

// Work is invocation-scoped protected work managed by WithLock. The context is
// canceled when authority or the caller is lost, and epochID must be enforced
// as a fencing token by protected resources that require stale-writer safety.
type Work func(context.Context, uint64) error

// Mutex is reusable sequentially. One WithLock lifecycle or manual Lock may be
// active at a time; a work-join timeout makes the instance permanently busy.
type Mutex struct {
	config Config
	// mu prevents concurrent WithLock lifecycles from both observing busy=false
	// and starting protected work through the same reusable recipe instance.
	mu   xsync.RBMutex
	busy bool
	// manual is non-nil only for a successful TryLock acquisition. Scoped
	// WithLock work remains entirely owned by the holder lifecycle.
	manual    *Lock
	releasing bool
}

// New validates config and returns a reusable distributed mutex.
func New(config Config) (_ *Mutex, err error) {
	logger := config.Logger
	if logger == nil {
		logger = common.Logger()
	}
	logger.Debug("mutex construction started")
	defer func() {
		if err != nil {
			logger.Error("mutex construction failed", zap.Error(err))
		}
	}()
	if isNil(config.Client) {
		return nil, fmt.Errorf("%w: lease client is required", lease.ErrInvalidConfig)
	}
	if config.RetryPeriod <= 0 || config.ObserveInterval <= 0 || config.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("%w: mutex timing values must be positive", lease.ErrInvalidConfig)
	}
	timing := config.Client.Timing()
	if timing.RenewDeadline <= config.RetryPeriod ||
		timing.RenewDeadline-config.RetryPeriod <= config.RetryPeriod/5 {
		return nil, fmt.Errorf("%w: renew deadline must exceed 1.2 times retry period", lease.ErrInvalidConfig)
	}
	if isNil(config.Metrics) {
		config.Metrics = noopMetrics{}
	}
	config.Logger = logger
	logger.Info("mutex constructed",
		zap.Duration("retry_period", config.RetryPeriod),
		zap.Duration("observe_interval", config.ObserveInterval),
		zap.Duration("shutdown_timeout", config.ShutdownTimeout),
		zap.Bool("release_on_cancel", config.ReleaseOnCancel))
	return &Mutex{config: config}, nil
}

// WithLock waits for a confirmed lease, runs work under automatic renewal,
// and releases only after the protected work has joined.
func (m *Mutex) WithLock(ctx context.Context, work Work) (err error) {
	if m == nil {
		return fmt.Errorf("%w: nil mutex", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if work == nil {
		return fmt.Errorf("%w: work is required", lease.ErrInvalidConfig)
	}
	if !m.enter() {
		return ErrRecipeBusy
	}
	m.config.Logger.Debug("mutex acquisition started")

	acquired, err := m.acquire(ctx)
	if err != nil {
		m.clearBusy()
		m.logError(err)
		return err
	}
	epochID := acquired.EpochID()
	err = holder.Run(ctx, acquired, holder.Work(work), holder.Policy{
		Client:              m.config.Client,
		RetryPeriod:         m.config.RetryPeriod,
		ShutdownTimeout:     m.config.ShutdownTimeout,
		ReleaseOnWorkReturn: true,
		ReleaseOnCancel:     m.config.ReleaseOnCancel,
		LossError:           ErrLeaseLost,
		WorkNotStoppedError: ErrWorkNotStopped,
		OnStarted: func() {
			m.config.Metrics.LockChanged(true, epochID)
			m.config.Logger.Info("mutex lock acquired", zap.Uint64("epoch_id", epochID))
		},
		OnStopped: func() {
			m.config.Metrics.LockChanged(false, epochID)
			m.config.Logger.Info("mutex protected work stopping", zap.Uint64("epoch_id", epochID))
		},
		OnShutdown: m.config.Metrics.WorkShutdown,
	})
	m.finish(acquired, err)
	if err == nil {
		m.config.Logger.Info("mutex lifecycle completed", zap.Uint64("epoch_id", epochID))
	} else {
		m.logError(err)
	}
	return err
}

func (m *Mutex) acquire(ctx context.Context) (*lease.Lease, error) {
	return acquire.Run(ctx, acquire.Config{
		Client:          m.config.Client,
		RetryPeriod:     m.config.RetryPeriod,
		ObserveInterval: m.config.ObserveInterval,
	})
}

func (m *Mutex) enter() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.busy {
		return false
	}
	m.busy = true
	return true
}

func (m *Mutex) finish(acquired *lease.Lease, runErr error) {
	if errors.Is(runErr, ErrWorkNotStopped) {
		return
	}
	if acquired.Check() == nil {
		go func() {
			<-acquired.Done()
			m.clearBusy()
		}()
		return
	}
	m.clearBusy()
}

func (m *Mutex) clearBusy() {
	m.mu.Lock()
	m.busy = false
	m.manual = nil
	m.releasing = false
	m.mu.Unlock()
}

func (m *Mutex) logError(err error) {
	if err != nil {
		m.config.Logger.Error("mutex lifecycle failed", zap.Error(err))
	}
}

func isNil(value any) bool {
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
