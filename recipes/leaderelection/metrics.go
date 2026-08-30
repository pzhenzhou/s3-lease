package leaderelection

import "time"

// LeaderMetric is a point-in-time process-local leadership snapshot.
type LeaderMetric struct {
	Held    bool
	EpochID uint64
}

// ShutdownMetric describes one tracked-work completion barrier.
type ShutdownMetric struct {
	Duration time.Duration
	TimedOut bool
}

// Metrics is a process-lifetime service owned by one Elector. Metric values are
// event-scoped snapshots.
type Metrics interface {
	LeaderChanged(LeaderMetric)
	ObservationCoalesced()
	WorkShutdown(ShutdownMetric)
}

type noopMetrics struct{}

func (noopMetrics) LeaderChanged(LeaderMetric) {}

func (noopMetrics) ObservationCoalesced() {}

func (noopMetrics) WorkShutdown(ShutdownMetric) {}
