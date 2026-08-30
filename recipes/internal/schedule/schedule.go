// Package schedule owns recipe polling and jitter policy.
package schedule

import (
	"math/rand/v2"
	"time"
)

const MaxPositiveJitter = 0.20

// Delay returns base with up to twenty percent positive jitter. Safety
// deadlines are deliberately not passed through this helper.
func Delay(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	maximum := base / 5
	if remaining := time.Duration(1<<63-1) - base; maximum > remaining {
		maximum = remaining
	}
	if maximum <= 0 {
		return base
	}
	return base + time.Duration(rand.Int64N(int64(maximum)+1))
}
