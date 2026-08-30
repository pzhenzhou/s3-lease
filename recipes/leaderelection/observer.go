package leaderelection

import (
	"context"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
)

// observerDispatcher is run-scoped and owned by one Elector. It permits one
// executing callback and one replaceable pending snapshot.
type observerDispatcher struct {
	mu       xsync.RBMutex
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(context.Context, lease.Observation)
	metrics  Metrics
	pending  *lease.Observation
	last     lease.Observation
	seen     bool
	running  bool
	stopped  bool
}

func newObserverDispatcher(parent context.Context, callback func(context.Context, lease.Observation), metrics Metrics) *observerDispatcher {
	ctx, cancel := context.WithCancel(parent)
	return &observerDispatcher{ctx: ctx, cancel: cancel, callback: callback, metrics: metrics}
}

// Submit filters renewal-only changes and enqueues the newest ownership
// transition without ever running application code on the polling goroutine.
func (d *observerDispatcher) Submit(observation lease.Observation) {
	if d == nil || d.callback == nil {
		return
	}
	d.mu.Lock()
	if d.stopped || d.seen && d.last.ClientID == observation.ClientID && d.last.EpochID == observation.EpochID {
		d.mu.Unlock()
		return
	}
	d.last = observation
	d.seen = true
	if d.running {
		copy := observation
		if d.pending != nil {
			d.metrics.ObservationCoalesced()
		}
		d.pending = &copy
		d.mu.Unlock()
		return
	}
	d.running = true
	d.mu.Unlock()
	go d.deliver(observation)
}

func (d *observerDispatcher) deliver(observation lease.Observation) {
	for {
		d.callback(d.ctx, observation)
		d.mu.Lock()
		if d.stopped || d.pending == nil {
			d.running = false
			d.mu.Unlock()
			return
		}
		observation = *d.pending
		d.pending = nil
		d.mu.Unlock()
	}
}

// Stop cancels callback context, discards pending snapshots, and returns
// without joining an already executing application callback.
func (d *observerDispatcher) Stop() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	d.pending = nil
	d.cancel()
	d.mu.Unlock()
}
