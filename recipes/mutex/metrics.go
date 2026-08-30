package mutex

import "time"

// Metrics is a process-lifetime service owned by one Mutex. Metric values are
// event-scoped snapshots.
type Metrics interface {
	LockChanged(held bool, epochID uint64)
	WorkShutdown(duration time.Duration, timedOut bool)
}

type noopMetrics struct{}

func (noopMetrics) LockChanged(bool, uint64) {}

func (noopMetrics) WorkShutdown(time.Duration, bool) {}
