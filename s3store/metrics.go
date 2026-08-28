package s3store

import "time"

type RequestOperation string

const (
	RequestGet    RequestOperation = "get"
	RequestCreate RequestOperation = "create"
	RequestCAS    RequestOperation = "compare_and_swap"
)

type RequestOutcome string

const (
	RequestSuccess      RequestOutcome = "success"
	RequestNotFound     RequestOutcome = "not_found"
	RequestConflict     RequestOutcome = "conflict"
	RequestUnauthorized RequestOutcome = "unauthorized"
	RequestTemporary    RequestOutcome = "temporary"
	RequestUnknown      RequestOutcome = "outcome_unknown"
	RequestInvalid      RequestOutcome = "invalid_response"
)

// RequestMetric is emitted for one completed physical S3 request.
type RequestMetric struct {
	Operation RequestOperation
	Outcome   RequestOutcome
	Duration  time.Duration
}

// RequestMetrics is a process-lifetime service owned by an adapter. Metric
// values are call-scoped snapshots.
type RequestMetrics interface {
	RequestCompleted(RequestMetric)
}
