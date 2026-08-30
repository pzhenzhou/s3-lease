// Package lease implements an S3-backed lease protocol.
//
// A Client is bound to one storage backend, bucket, object key, and stable
// client ID. Require performs one acquisition attempt; recipes provide retry
// loops and automatic renewal.
package lease

import (
	"context"
	"time"

	"go.uber.org/zap"
)

const (
	APIVersion = "coordination.pactdata.io/v1alpha1"
	Kind       = "Lease"
)

// Version is mutation-scoped opaque storage state. It remains valid only while
// the corresponding object version is current and must not be parsed or
// ordered.
type Version string

// Key has coordination-lifetime identity. Every participant must retain the
// exact bucket/full-key pair for the lifetime of the coordinated resource.
type Key struct {
	Bucket    string
	ObjectKey string
}

// StoredObject is a read-scoped immutable snapshot returned by LeaseStore.
type StoredObject struct {
	Body    []byte
	Version Version
}

// LeaseStore is a process-lifetime conditional object-storage service required
// by the core. Implementations must not modify write bodies and must preserve
// Version values exactly.
type LeaseStore interface {
	Get(ctx context.Context, key Key) (StoredObject, error)
	// CreateIfAbsent atomically creates key only when no current object exists.
	// An existing object must be reported as a precondition failure or conflict.
	CreateIfAbsent(ctx context.Context, key Key, body []byte) (Version, error)
	CompareAndSwap(ctx context.Context, key Key, expected Version, body []byte) (Version, error)
}

// Clock is a process-lifetime time service. It supplies monotonic-capable time
// and local callback scheduling. AfterFunc returns an idempotent function that
// stops the callback; no timer abstraction crosses the module boundary.
type Clock interface {
	Now() time.Time
	AfterFunc(delay time.Duration, fn func()) (stop func() bool)
}

// Config is construction-scoped configuration. A Client implementation
// copies it at construction and treats it as immutable for that instance's
// lifetime.
type Config struct {
	Store          LeaseStore
	Key            Key
	ClientID       string
	MetadataName   string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RequestTimeout time.Duration
	Clock          Clock
	Metrics        Metrics
	Logger         *zap.Logger
}

// Timing is process-lifetime immutable configuration metadata.
type Timing struct {
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RequestTimeout time.Duration
}

// Observation is a read-scoped value snapshot. It may outlive its source
// Client, but it never grants authority and cannot reconstruct a Lease.
type Observation struct {
	LeaseUID   string
	ClientID   string
	EpochID    uint64
	SequenceID uint64
	ReadAt     time.Time
}

// Client is a process-local coordination service. One instance is bound to one
// store/key/client ID and may serve multiple sequential acquisitions. It must
// not be copied or restored from persisted state.
type Client interface {
	Require(ctx context.Context) (*Lease, error)
	Renew(ctx context.Context, acquired *Lease) error
	Release(ctx context.Context, acquired *Lease) error
	Observe(ctx context.Context) (Observation, error)
	Timing() Timing
}
