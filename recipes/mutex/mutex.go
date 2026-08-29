// Package mutex defines a scoped distributed-lock recipe over the lease core.
package mutex

import (
	"context"
	"errors"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
	"go.uber.org/zap"
)

var (
	ErrRecipeBusy     = errors.New("mutex recipe is busy")
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

// Work is invocation-scoped protected work.
type Work func(context.Context, uint64) error

// Mutex is reusable sequentially. One WithLock lifecycle may be active at a
// time; a work-join timeout makes the instance permanently busy.
//
// Planned public operation: WithLock(context.Context, Work) error.
type Mutex struct {
	config Config
	// mu prevents concurrent WithLock lifecycles from both observing busy=false
	// and starting protected work through the same reusable recipe instance.
	mu   xsync.RBMutex
	busy bool
}
