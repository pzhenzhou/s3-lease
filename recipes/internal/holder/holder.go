// Package holder defines the private lifecycle framework shared by recipes.
package holder

import (
	"context"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
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
	OnStopped           func()
	OnShutdown          func(duration time.Duration, timedOut bool)
}
