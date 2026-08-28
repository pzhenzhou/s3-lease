package fencedmanifest

import (
	"errors"

	"github.com/pzhenzhou/s3-lease/lease"
)

var ErrFenced = errors.New("manifest mutation fenced")

// Manifest has protected-resource lifetime. AcceptedEpochID is a durable
// non-decreasing watermark across all publications.
type Manifest struct {
	AcceptedEpochID uint64         `json:"acceptedEpochID"`
	Revision        uint64         `json:"revision"`
	Payload         map[string]any `json:"payload,omitempty"`
	History         []HistoryEntry `json:"history,omitempty"`
}

// HistoryEntry is an immutable audit value for one committed resource action.
type HistoryEntry struct {
	EpochID    uint64 `json:"epochID"`
	MutationID string `json:"mutationID"`
	Activation bool   `json:"activation,omitempty"`
}

// ManifestWriter is process-scoped application integration bound to one
// protected manifest. Individual activation and publication requests are
// call-scoped and carry an acquisition-scoped epoch.
//
// Planned operations are Activate and Publish; their resource-side CAS behavior
// is intentionally not implemented in this framework.
type ManifestWriter struct {
	Store lease.LeaseStore
	Key   lease.Key
}
