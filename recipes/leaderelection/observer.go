package leaderelection

import (
	"context"
	"sync"

	"github.com/pzhenzhou/s3-lease/lease"
)

// observerDispatcher is run-scoped and owned by one Elector. It permits one
// executing callback and one replaceable pending snapshot. It deliberately is
// not exposed as another interface.
type observerDispatcher struct {
	mu       sync.Mutex
	callback func(context.Context, lease.Observation)
	pending  *lease.Observation
	running  bool
	stopped  bool
}
