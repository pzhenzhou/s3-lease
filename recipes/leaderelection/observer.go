package leaderelection

import (
	"context"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
)

// observerDispatcher is run-scoped and owned by one Elector. It permits one
// executing callback and one replaceable pending snapshot. It deliberately is
// not exposed as another interface.
type observerDispatcher struct {
	// mu serializes polling producers with callback completion while they
	// replace the coalesced pending observation and transition running/stopped.
	mu       xsync.RBMutex
	callback func(context.Context, lease.Observation)
	pending  *lease.Observation
	running  bool
	stopped  bool
}
