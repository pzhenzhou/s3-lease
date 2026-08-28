// Package leaderelection defines a single-use leader-election recipe over the
// lease core.
package leaderelection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
)

var (
	ErrRunAlreadyUsed = errors.New("leader elector has already been used")
	ErrLeadershipLost = errors.New("leadership lost")
	ErrWorkNotStopped = errors.New("leader work did not stop")
)

// Config is construction-scoped and immutable for one Elector run.
type Config struct {
	Lease           lease.Lease
	RetryPeriod     time.Duration
	ObserveInterval time.Duration
	ShutdownTimeout time.Duration
	ReleaseOnCancel bool
	Callbacks       Callbacks
	Metrics         Metrics
	Logger          *slog.Logger
}

// Callbacks is run-scoped configuration, not a service interface. The start
// hook is required; the stop and observation hooks are optional.
type Callbacks struct {
	OnStartedLeading func(context.Context, uint64) error
	OnStoppedLeading func()
	OnLeaderObserved func(context.Context, lease.Observation)
}

// Elector is single-use. Its lifetime begins at construction and ends when its
// sole Run lifecycle has stopped and tracked leader work has joined or timed
// out. It is never reset or reused.
//
// Planned public operation: Run(context.Context) error.
type Elector struct {
	config Config
	mu     sync.Mutex
	used   bool
}
