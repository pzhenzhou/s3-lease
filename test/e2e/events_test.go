//go:build e2e

package e2e

import "github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"

func countEvent(events []harness.Event, message string) int {
	count := 0
	for _, event := range events {
		if event.Message == message {
			count++
		}
	}
	return count
}

func firstEvent(events []harness.Event, message string) (harness.Event, bool) {
	for _, event := range events {
		if event.Message == message {
			return event, true
		}
	}
	return harness.Event{}, false
}
