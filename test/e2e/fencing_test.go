//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/fencedresource"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"
)

func TestPausedOldLeaderIsRejectedAfterReplacementActivation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leaseKey := testHarness.Key("paused-fenced-lease")
	resourceKey := testHarness.Key("paused-fenced-resource")
	oldLeader, err := testHarness.StartCandidate(ctx, electionCandidateArgs(leaseKey, "paused-old", 3*time.Second, 2*time.Second, 20*time.Second)...)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent, err := oldLeader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	resource := fencedresource.New(store, lease.Key{Bucket: testHarness.Bucket, ObjectKey: resourceKey})
	if _, err := resource.Apply(ctx, oldEvent.EpochID, "old-before-pause"); err != nil {
		t.Fatal(err)
	}
	if err := oldLeader.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	newLeader, err := testHarness.StartCandidate(ctx, electionCandidateArgs(leaseKey, "replacement", 3*time.Second, 2*time.Second, time.Second)...)
	if err != nil {
		t.Fatal(err)
	}
	newEvent, err := newLeader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	})
	if err != nil {
		t.Fatal(err)
	}
	if newEvent.EpochID <= oldEvent.EpochID {
		t.Fatalf("replacement epoch %d did not exceed paused epoch %d", newEvent.EpochID, oldEvent.EpochID)
	}
	if _, err := resource.Apply(ctx, newEvent.EpochID, "replacement-activated"); err != nil {
		t.Fatal(err)
	}
	if err := oldLeader.Unpause(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Apply(ctx, oldEvent.EpochID, "resumed-stale-mutation"); !errors.Is(err, fencedresource.ErrFenced) {
		t.Fatalf("resumed stale mutation = %v, want ErrFenced", err)
	}
	_, _ = oldLeader.Wait()
	if output, err := newLeader.Wait(); err != nil {
		t.Fatalf("replacement leader: %v\n%s", err, output)
	}
	state, err := resource.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.EpochWatermark != newEvent.EpochID || len(state.History) != 2 {
		t.Fatalf("fenced resource accepted stale history: %+v", state)
	}
}

func TestFencedResourceRejectsStaleEpochAndSurvivesStoreRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	leaseKey := testHarness.Key("fenced-lease")
	resourceKey := testHarness.Key("fenced-resource")
	oldClient := newClient(t, leaseKey, "old-leader", 3*time.Second, 2*time.Second)
	oldLease, err := oldClient.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	resource := fencedresource.New(store, lease.Key{Bucket: testHarness.Bucket, ObjectKey: resourceKey})
	if _, err := resource.Apply(ctx, oldLease.EpochID(), "old-before-replacement"); err != nil {
		t.Fatal(err)
	}
	if err := oldClient.Release(ctx, oldLease); err != nil {
		t.Fatal(err)
	}

	successorClient := newClient(t, leaseKey, "new-leader", 3*time.Second, 2*time.Second)
	newLease, err := successorClient.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.EpochID() != 2 {
		t.Fatalf("replacement epoch = %d, want 2", newLease.EpochID())
	}
	if _, err := resource.Apply(ctx, newLease.EpochID(), "replacement-activated"); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Apply(ctx, oldLease.EpochID(), "resumed-stale-write"); !errors.Is(err, fencedresource.ErrFenced) {
		t.Fatalf("stale mutation = %v, want ErrFenced", err)
	}
	beforeRestart, err := successorClient.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := successorClient.Release(ctx, newLease); err != nil {
		t.Fatal(err)
	}
	if err := testHarness.RestartStore(ctx); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := newClient(t, leaseKey, "restart-inspector", 3*time.Second, 2*time.Second).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterRestart.LeaseUID != beforeRestart.LeaseUID || afterRestart.EpochID != 2 || afterRestart.ClientID != "" {
		t.Fatalf("lease state changed across restart: before=%+v after=%+v", beforeRestart, afterRestart)
	}
	state, err := resource.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.EpochWatermark != 2 || len(state.History) != 2 {
		t.Fatalf("protected history after restart = %+v", state)
	}
}
