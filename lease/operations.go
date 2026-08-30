package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

func (c *core) Require(ctx context.Context) (_ *Lease, err error) {
	c.logger.Debug("lease require started")
	defer func() { c.logMethodError("require", err) }()
	if !c.mutationMu.TryLock() {
		return nil, ErrConcurrentMutation
	}
	defer c.mutationMu.Unlock()
	if err = c.prepareRequire(); err != nil {
		return nil, err
	}

	current, err := c.readCurrent(ctx, time.Time{})
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	now := c.config.Clock.Now()
	acquisition := &proposal{kind: proposalAcquire}
	create := errors.Is(err, ErrNotFound)
	if create {
		acquisition.record, err = newInitialRecord(c.config, now)
	} else {
		if !eligible(current, now) {
			return nil, ErrNotEligible
		}
		acquisition.expected = current.version
		acquisition.record, err = acquisitionRecord(current.record, c.config, now)
	}
	if err != nil {
		return nil, err
	}
	acquisition.body, err = encodeRecord(acquisition.record)
	if err != nil {
		return nil, err
	}
	acquisition.firstSendAt = c.config.Clock.Now()
	c.logger.Info("lease acquisition proposal frozen",
		zap.Bool("create", create),
		zap.Uint64("epoch_id", acquisition.record.Spec.EpochID),
		zap.Uint64("sequence_id", acquisition.record.Spec.SequenceID),
		zap.String("expected_version", string(acquisition.expected)))

	phaseEnd := acquisition.firstSendAt.Add(c.config.RenewDeadline)
	requestCtx, cancel, err := c.requestContext(ctx, phaseEnd)
	if err != nil {
		return nil, err
	}
	var version Version
	if create {
		version, err = c.config.Store.CreateIfAbsent(requestCtx, c.config.Key, acquisition.body)
	} else {
		version, err = c.config.Store.CompareAndSwap(requestCtx, c.config.Key, acquisition.expected, acquisition.body)
	}
	cancel()
	if err != nil {
		return nil, classifyAcquisitionError(err)
	}
	if version == "" {
		return nil, fmt.Errorf("%w: acquisition response omitted version", ErrUnknownOutcome)
	}
	return c.confirmAcquisition(ctx, acquisition, version)
}

func (c *core) Renew(ctx context.Context, acquired *Lease) (err error) {
	c.logger.Debug("lease renew started", zap.Uint64("epoch_id", acquiredEpoch(acquired)))
	defer func() { c.logMethodError("renew", err) }()
	if !c.mutationMu.TryLock() {
		return ErrConcurrentMutation
	}
	defer c.mutationMu.Unlock()

	pending, err := c.pendingFor(acquired, proposalRenew)
	if err != nil {
		return err
	}
	if pending != nil {
		c.logger.Info("reconciling unresolved lease renewal",
			zap.Uint64("epoch_id", pending.record.Spec.EpochID),
			zap.Uint64("sequence_id", pending.record.Spec.SequenceID))
		return c.reconcilePending(ctx, pending)
	}

	c.stateMu.Lock()
	if err = c.validateActiveLocked(acquired); err != nil {
		c.stateMu.Unlock()
		return err
	}
	record := c.observed.record
	expected := c.observed.version
	authorityEnds := acquired.validUntil
	c.stateMu.Unlock()

	next, err := renewalRecord(record, c.config.Clock.Now())
	if err != nil {
		return err
	}
	body, err := encodeRecord(next)
	if err != nil {
		return err
	}
	renewal := &proposal{
		kind:          proposalRenew,
		acquired:      acquired,
		record:        next,
		body:          body,
		expected:      expected,
		firstSendAt:   c.config.Clock.Now(),
		authorityEnds: authorityEnds,
	}
	c.stateMu.Lock()
	if err = c.validateActiveLocked(acquired); err == nil {
		c.pending = renewal
	}
	c.stateMu.Unlock()
	if err != nil {
		return err
	}
	c.logger.Info("lease renewal proposal frozen",
		zap.Uint64("epoch_id", next.Spec.EpochID),
		zap.Uint64("sequence_id", next.Spec.SequenceID),
		zap.String("expected_version", string(expected)))
	return c.sendPending(ctx, renewal)
}

func (c *core) Release(ctx context.Context, acquired *Lease) (err error) {
	c.logger.Debug("lease release started", zap.Uint64("epoch_id", acquiredEpoch(acquired)))
	defer func() { c.logMethodError("release", err) }()
	if !c.mutationMu.TryLock() {
		return ErrConcurrentMutation
	}
	defer c.mutationMu.Unlock()

	pending, err := c.pendingFor(acquired, proposalRelease)
	if err != nil {
		return err
	}
	if pending != nil {
		c.logger.Info("reconciling unresolved lease release",
			zap.Uint64("epoch_id", pending.record.Spec.EpochID),
			zap.Uint64("sequence_id", pending.record.Spec.SequenceID))
		return c.reconcilePending(ctx, pending)
	}

	c.stateMu.Lock()
	if err = c.validateActiveLocked(acquired); err != nil {
		c.stateMu.Unlock()
		return err
	}
	record := c.observed.record
	expected := c.observed.version
	authorityEnds := acquired.validUntil
	c.retireLeaseLocked(acquired, ErrLeaseRetired)
	c.stateMu.Unlock()

	next, err := releaseRecord(record, c.config.Clock.Now())
	if err != nil {
		return err
	}
	body, err := encodeRecord(next)
	if err != nil {
		return err
	}
	release := &proposal{
		kind:          proposalRelease,
		acquired:      acquired,
		record:        next,
		body:          body,
		expected:      expected,
		firstSendAt:   c.config.Clock.Now(),
		authorityEnds: authorityEnds,
	}
	c.stateMu.Lock()
	c.pending = release
	c.stateMu.Unlock()
	c.logger.Info("lease release proposal frozen",
		zap.Uint64("epoch_id", next.Spec.EpochID),
		zap.Uint64("sequence_id", next.Spec.SequenceID),
		zap.String("expected_version", string(expected)))
	return c.sendPending(ctx, release)
}

func (c *core) Observe(ctx context.Context) (_ Observation, err error) {
	c.logger.Debug("lease observe started")
	defer func() { c.logMethodError("observe", err) }()
	state, err := c.readCurrent(ctx, time.Time{})
	if err != nil {
		return Observation{}, err
	}
	observation := Observation{
		LeaseUID:   state.record.Metadata.UID,
		ClientID:   state.record.Spec.ClientID,
		EpochID:    state.record.Spec.EpochID,
		SequenceID: state.record.Spec.SequenceID,
		ReadAt:     state.lastReadAt,
	}
	c.logger.Debug("lease observation updated",
		zap.String("observed_client_id", observation.ClientID),
		zap.Uint64("epoch_id", observation.EpochID),
		zap.Uint64("sequence_id", observation.SequenceID),
		zap.String("version", string(state.version)))
	return observation, nil
}

func (c *core) prepareRequire() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	now := c.config.Clock.Now()
	if c.active != nil && c.active.retired == nil && !now.Before(c.active.validUntil) {
		c.retireLeaseLocked(c.active, ErrLeaseExpired)
	}
	if c.pending != nil && !now.Before(c.proposalPhaseEnd(c.pending)) {
		c.abandonExpiredPendingLocked(c.pending)
	}
	if c.active != nil {
		return ErrAlreadyHeld
	}
	if c.pending != nil {
		return fmt.Errorf("%w: unresolved %s proposal", ErrUnknownOutcome, c.pending.kind)
	}
	return nil
}

func (c *core) pendingFor(acquired *Lease, kind proposalKind) (*proposal, error) {
	if acquired == nil || acquired.origin != c {
		return nil, ErrInvalidLease
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.terminal != nil {
		return nil, c.terminal
	}
	if c.pending != nil {
		if c.pending.acquired != acquired || c.pending.kind != kind {
			return nil, ErrInvalidLease
		}
		return c.pending, nil
	}
	return nil, nil
}

func (c *core) validateActiveLocked(acquired *Lease) error {
	if acquired == nil || acquired.origin != c {
		return ErrInvalidLease
	}
	if c.terminal != nil {
		return c.terminal
	}
	if acquired.retired != nil {
		return acquired.retired
	}
	if c.active != acquired {
		return ErrInvalidLease
	}
	if !c.config.Clock.Now().Before(acquired.validUntil) {
		c.retireLeaseLocked(acquired, ErrLeaseExpired)
		return ErrLeaseExpired
	}
	if c.observed == nil || c.observed.record.Metadata.UID != acquired.leaseUID ||
		c.observed.record.Spec.ClientID != acquired.clientID ||
		c.observed.record.Spec.EpochID != acquired.epochID ||
		c.observed.record.Spec.SequenceID != acquired.sequenceID {
		c.retireLeaseLocked(acquired, ErrOwnershipLost)
		return ErrOwnershipLost
	}
	return nil
}

func (c *core) readCurrent(ctx context.Context, phaseEnd time.Time) (*observedState, error) {
	token := c.stateMu.RLock()
	startedFrom := c.observed
	terminal := c.terminal
	c.stateMu.RUnlock(token)
	if terminal != nil {
		return nil, terminal
	}
	requestCtx, cancel, err := c.requestContext(ctx, phaseEnd)
	if err != nil {
		return nil, err
	}
	object, err := c.config.Store.Get(requestCtx, c.config.Key)
	cancel()
	readAt := c.config.Clock.Now()
	if err != nil {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		if c.terminal != nil {
			return nil, c.terminal
		}
		if c.observed != startedFrom {
			return cloneObserved(c.observed), nil
		}
		if errors.Is(err, ErrNotFound) {
			if c.observed != nil {
				return nil, c.setTerminalLocked(fmt.Errorf("%w: %v", ErrLeaseDisappeared, err))
			}
			return nil, ErrNotFound
		}
		return nil, err
	}
	if object.Version == "" {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		if c.terminal != nil {
			return nil, c.terminal
		}
		if c.observed != startedFrom {
			return cloneObserved(c.observed), nil
		}
		return nil, c.setTerminalLocked(fmt.Errorf("%w: read response omitted version", ErrProtocolViolation))
	}
	record, err := decodeRecord(object.Body)
	if err != nil {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		if c.terminal != nil {
			return nil, c.terminal
		}
		if c.observed != startedFrom {
			return cloneObserved(c.observed), nil
		}
		return nil, c.setTerminalLocked(err)
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.terminal != nil {
		return nil, c.terminal
	}
	// Every installed observation is a fresh pointer. Holding startedFrom keeps
	// its allocation live, so pointer inequality is an ABA-safe generation
	// check for a newer validated read or confirmed local mutation.
	if c.observed != startedFrom {
		return cloneObserved(c.observed), nil
	}
	unchangedSince := readAt
	if c.observed != nil {
		if err := recordsCompatible(c.observed.record, record); err != nil {
			return nil, c.setTerminalLocked(err)
		}
		if object.Version == c.observed.version {
			if !sameRecord(c.observed.record, record) {
				return nil, c.setTerminalLocked(fmt.Errorf("%w: one version returned different records", ErrProtocolViolation))
			}
			unchangedSince = c.observed.unchangedSince
		}
	}
	c.observed = &observedState{
		record:         record,
		body:           append([]byte(nil), object.Body...),
		version:        object.Version,
		unchangedSince: unchangedSince,
		lastReadAt:     readAt,
	}
	c.retireIfObservationLostLocked(record)
	return cloneObserved(c.observed), nil
}

func (c *core) retireIfObservationLostLocked(record Record) {
	if c.active == nil || c.active.retired != nil {
		return
	}
	active := c.active
	if record.Metadata.UID != active.leaseUID || record.Spec.ClientID != active.clientID ||
		record.Spec.EpochID != active.epochID {
		c.retireLeaseLocked(active, ErrOwnershipLost)
		return
	}
	if record.Spec.SequenceID == active.sequenceID {
		return
	}
	if c.pending != nil && c.pending.kind == proposalRenew && c.pending.acquired == active &&
		sameRecord(record, c.pending.record) {
		return
	}
	c.retireLeaseLocked(active, ErrOwnershipLost)
}

func cloneObserved(state *observedState) *observedState {
	if state == nil {
		return nil
	}
	clone := *state
	clone.body = append([]byte(nil), state.body...)
	return &clone
}

func eligible(state *observedState, now time.Time) bool {
	if state.record.Spec.ClientID == "" {
		return true
	}
	duration := time.Duration(state.record.Spec.LeaseDurationSeconds) * time.Second
	return now.Sub(state.unchangedSince) >= duration
}

func (c *core) confirmAcquisition(ctx context.Context, acquisition *proposal, version Version) (*Lease, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.terminal != nil {
		return nil, c.terminal
	}
	deadline := c.proposalPhaseEnd(acquisition)
	if ctx.Err() != nil || !c.config.Clock.Now().Before(deadline) {
		return nil, fmt.Errorf("%w: acquisition succeeded after its authority deadline", ErrUnknownOutcome)
	}
	if c.observed != nil {
		current := c.observed.record
		if current.Metadata.UID != acquisition.record.Metadata.UID {
			return nil, ErrOwnershipLost
		}
		if current.Spec.EpochID > acquisition.record.Spec.EpochID ||
			(current.Spec.EpochID == acquisition.record.Spec.EpochID && current.Spec.SequenceID > acquisition.record.Spec.SequenceID) {
			return nil, ErrOwnershipLost
		}
		if current.Spec.EpochID == acquisition.record.Spec.EpochID &&
			current.Spec.SequenceID == acquisition.record.Spec.SequenceID &&
			(!sameRecord(current, acquisition.record) || c.observed.version != version) {
			return nil, ErrOwnershipLost
		}
	}
	acquired := &Lease{
		origin:     c,
		leaseUID:   acquisition.record.Metadata.UID,
		clientID:   c.config.ClientID,
		epochID:    acquisition.record.Spec.EpochID,
		sequenceID: acquisition.record.Spec.SequenceID,
		validUntil: deadline,
		done:       make(chan struct{}),
	}
	lastReadAt := time.Time{}
	if c.observed != nil {
		lastReadAt = c.observed.lastReadAt
	}
	c.observed = &observedState{
		record:         acquisition.record,
		body:           append([]byte(nil), acquisition.body...),
		version:        version,
		unchangedSince: acquisition.firstSendAt,
		lastReadAt:     lastReadAt,
	}
	c.active = acquired
	c.installTimerLocked(acquired)
	c.logger.Info("lease acquisition confirmed",
		zap.Uint64("epoch_id", acquired.epochID),
		zap.Time("valid_until", acquired.validUntil),
		zap.String("version", string(version)))
	return acquired, nil
}

func (c *core) reconcilePending(ctx context.Context, pending *proposal) error {
	phaseEnd := c.proposalPhaseEnd(pending)
	if !c.config.Clock.Now().Before(phaseEnd) {
		return c.abandonExpiredPending(pending)
	}
	current, err := c.readCurrent(ctx, phaseEnd)
	if err != nil {
		return err
	}
	resolved, err := c.applyPendingReadback(pending, current)
	if resolved || err != nil {
		return err
	}
	return c.sendPending(ctx, pending)
}

func (c *core) sendPending(ctx context.Context, pending *proposal) error {
	phaseEnd := c.proposalPhaseEnd(pending)
	requestCtx, cancel, err := c.requestContext(ctx, phaseEnd)
	if err != nil {
		if !c.config.Clock.Now().Before(phaseEnd) {
			return c.abandonExpiredPending(pending)
		}
		c.clearPending(pending)
		return err
	}
	version, err := c.config.Store.CompareAndSwap(requestCtx, c.config.Key, pending.expected, pending.body)
	cancel()
	if err != nil {
		if isUnknownWrite(err) {
			return fmt.Errorf("%w: %w", ErrUnknownOutcome, err)
		}
		if isDefinitiveConflict(err) {
			return c.reconcileConflict(ctx, pending)
		}
		c.clearPending(pending)
		return err
	}
	if version == "" {
		return fmt.Errorf("%w: mutation response omitted version", ErrUnknownOutcome)
	}
	if pending.kind == proposalRenew {
		return c.confirmRenewal(pending, version)
	}
	return c.confirmRelease(pending, version)
}

func (c *core) reconcileConflict(ctx context.Context, pending *proposal) error {
	phaseEnd := c.proposalPhaseEnd(pending)
	if !c.config.Clock.Now().Before(phaseEnd) {
		return c.abandonExpiredPending(pending)
	}
	current, err := c.readCurrent(ctx, phaseEnd)
	if err != nil {
		return err
	}
	resolved, err := c.applyPendingReadback(pending, current)
	if resolved || err != nil {
		return err
	}
	// A retry's 412 may hide an earlier successful attempt. If a strong
	// read still returns the predecessor, retain the immutable proposal for
	// a later exact reconciliation instead of claiming ownership loss.
	return fmt.Errorf("%w: conditional write rejected while its predecessor remains current", ErrUnknownOutcome)
}

func (c *core) applyPendingReadback(pending *proposal, current *observedState) (bool, error) {
	if sameRecord(current.record, pending.record) {
		if pending.kind == proposalRenew {
			return true, c.confirmRenewal(pending, current.version)
		}
		return true, c.confirmRelease(pending, current.version)
	}
	if current.version != pending.expected {
		return true, c.losePending(pending)
	}
	return false, nil
}

func (c *core) proposalPhaseEnd(pending *proposal) time.Time {
	proposalEnd := pending.firstSendAt.Add(c.config.RenewDeadline)
	if pending.authorityEnds.IsZero() || proposalEnd.Before(pending.authorityEnds) {
		return proposalEnd
	}
	return pending.authorityEnds
}

func (c *core) confirmRenewal(renewal *proposal, version Version) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	now := c.config.Clock.Now()
	proposalEnd := renewal.firstSendAt.Add(c.config.RenewDeadline)
	if c.pending != renewal || c.active != renewal.acquired || renewal.acquired.retired != nil ||
		!now.Before(renewal.authorityEnds) || !now.Before(proposalEnd) {
		if c.pending == renewal {
			c.pending = nil
		}
		if renewal.acquired.retired == nil {
			c.retireLeaseLocked(renewal.acquired, ErrLeaseExpired)
		}
		return ErrLeaseExpired
	}
	lastReadAt := c.observed.lastReadAt
	c.observed = &observedState{
		record:         renewal.record,
		body:           append([]byte(nil), renewal.body...),
		version:        version,
		unchangedSince: renewal.firstSendAt,
		lastReadAt:     lastReadAt,
	}
	renewal.acquired.sequenceID = renewal.record.Spec.SequenceID
	renewal.acquired.validUntil = proposalEnd
	c.pending = nil
	c.installTimerLocked(renewal.acquired)
	c.logger.Info("lease renewal confirmed",
		zap.Uint64("epoch_id", renewal.record.Spec.EpochID),
		zap.Uint64("sequence_id", renewal.record.Spec.SequenceID),
		zap.Time("valid_until", proposalEnd),
		zap.String("version", string(version)))
	return nil
}

func (c *core) confirmRelease(release *proposal, version Version) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.pending != release {
		return ErrInvalidLease
	}
	lastReadAt := time.Time{}
	if c.observed != nil {
		lastReadAt = c.observed.lastReadAt
	}
	c.observed = &observedState{
		record:         release.record,
		body:           append([]byte(nil), release.body...),
		version:        version,
		unchangedSince: release.firstSendAt,
		lastReadAt:     lastReadAt,
	}
	c.pending = nil
	c.logger.Info("lease release confirmed",
		zap.Uint64("epoch_id", release.record.Spec.EpochID),
		zap.Uint64("sequence_id", release.record.Spec.SequenceID),
		zap.String("version", string(version)))
	return nil
}

func (c *core) abandonExpiredPending(pending *proposal) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.abandonExpiredPendingLocked(pending)
	return ErrLeaseExpired
}

func (c *core) abandonExpiredPendingLocked(pending *proposal) {
	if c.pending == pending {
		c.pending = nil
	}
	if pending.kind == proposalRenew && pending.acquired.retired == nil {
		c.retireLeaseLocked(pending.acquired, ErrLeaseExpired)
	}
}

func (c *core) losePending(pending *proposal) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.pending == pending {
		c.pending = nil
	}
	if pending.kind == proposalRenew && pending.acquired.retired == nil {
		c.retireLeaseLocked(pending.acquired, ErrOwnershipLost)
	}
	return ErrOwnershipLost
}

func (c *core) clearPending(pending *proposal) {
	c.stateMu.Lock()
	if c.pending == pending {
		c.pending = nil
	}
	c.stateMu.Unlock()
}

func classifyAcquisitionError(err error) error {
	if isUnknownWrite(err) {
		return fmt.Errorf("%w: %w", ErrUnknownOutcome, err)
	}
	if isDefinitiveConflict(err) {
		return fmt.Errorf("%w: %w", ErrConflict, err)
	}
	return err
}

func isUnknownWrite(err error) bool {
	return errors.Is(err, ErrUnknownOutcome) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func acquiredEpoch(acquired *Lease) uint64 {
	if acquired == nil {
		return 0
	}
	return acquired.epochID
}
