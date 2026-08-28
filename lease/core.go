package lease

import (
	"sync"
	"time"
)

// core is the planned Lease implementation. The state fields establish the
// ownership boundaries for observation, mutation reconciliation, and local
// authority without implementing the protocol in this framework version.
type core struct {
	config LeaseConfig

	mutationMu sync.Mutex
	stateMu    sync.Mutex
	observed   *observedState
	active     *Handle
	pending    *proposal
	terminal   error
}

// observedState lives for the lifetime of its owning core and is replaced only
// by a validated, non-regressing observation or confirmed local mutation.
type observedState struct {
	record         LeaseRecord
	body           []byte
	version        Version
	unchangedSince time.Time
	lastReadAt     time.Time
}

// proposal is mutation-scoped. It is frozen before the first write and retained
// unchanged until its outcome is confirmed or its deadline is abandoned.
type proposal struct {
	kind        proposalKind
	record      LeaseRecord
	body        []byte
	expected    Version
	firstSendAt time.Time
}

type proposalKind uint8

const (
	proposalAcquire proposalKind = iota + 1
	proposalRenew
	proposalRelease
)

// Handle is acquisition-scoped local authority. It originates from one
// confirmed Require result, belongs to one core, and is permanently retired by
// expiry, release, or loss. It is neither serializable nor transferable. Its
// planned read surface is EpochID, ValidUntil, Done, and Check.
type Handle struct {
	origin     *core
	leaseUID   string
	clientID   string
	epochID    uint64
	sequenceID uint64
	validUntil time.Time
	done       chan struct{}
}
