package mutex

import "time"

// Metrics is a process-lifetime service owned by one Mutex. Metric values are
// event-scoped snapshots.
type Metrics interface {
	LockChanged(held bool, epochID uint64)
	WorkShutdown(duration time.Duration, timedOut bool)
}
