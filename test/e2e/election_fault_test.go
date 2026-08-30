//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/leaderelection"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/faultproxy"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"
)

func TestElectionLeaderPartitionStopsLocallyBeforeTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("election-partition")
	leaderArgs := append(electionCandidateArgs(key, "partitioned-leader", 3*time.Second, 2*time.Second, 20*time.Second), "--endpoint", endpoint)
	leader, err := testHarness.StartCandidate(ctx, leaderArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	partitionedAt := time.Now()
	proxy.SetPartition(true)
	output, waitErr := leader.Wait()
	if waitErr == nil || !strings.Contains(string(output), leaderelection.ErrLeadershipLost.Error()) {
		t.Fatalf("partitioned leader = %v, logs:\n%s", waitErr, output)
	}
	if elapsed := time.Since(partitionedAt); elapsed > 3*time.Second {
		t.Fatalf("leader took %v to stop after a 2s authority deadline", elapsed)
	}
	if countEvent(leader.Events(), "candidate_election_work_stopped") != 1 {
		t.Fatalf("partition did not cancel tracked work exactly once:\n%s", output)
	}

	follower, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "partition-follower", 3*time.Second, 2*time.Second, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := follower.Wait(); err != nil {
		t.Fatalf("partition follower: %v\n%s", err, output)
	}
	assertElectionLifecycle(t, follower.Events(), 2)
	traces := proxy.Traces()
	if len(traces) == 0 {
		t.Fatal("partition proxy captured no requests")
	}
}

func TestCommittedButLostAcquisitionResponseNeverStartsWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	proxy.AddRule(faultproxy.Rule{Method: "PUT", IfNoneMatch: true, Outcome: faultproxy.OutcomeSuppressResponse})
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("lost-acquisition-response")
	args := append(electionCandidateArgs(key, "uncertain-candidate", 4*time.Second, 3*time.Second, 10*time.Second), "--endpoint", endpoint)
	candidate, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	observation := waitForOwner(t, ctx, newClient(t, key, "uncertain-inspector", 4*time.Second, 3*time.Second), "uncertain-candidate")
	if observation.EpochID != 1 {
		t.Fatalf("committed uncertain acquisition = %+v", observation)
	}
	noStartCtx, stopNoStart := context.WithTimeout(ctx, 900*time.Millisecond)
	_, startErr := candidate.WaitForEvent(noStartCtx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	})
	stopNoStart()
	if startErr == nil {
		t.Fatalf("candidate started work from a lost acquisition response:\n%s", candidate.Output())
	}
	if err := candidate.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = candidate.Wait()
	if countEvent(candidate.Events(), "candidate_election_work_started") != 0 {
		t.Fatalf("uncertain candidate started work:\n%s", candidate.Output())
	}
	traces := proxy.Traces()
	foundSuppressed := false
	for _, trace := range traces {
		foundSuppressed = foundSuppressed || trace.Outcome == faultproxy.OutcomeSuppressResponse && trace.Forwarded
	}
	if !foundSuppressed {
		t.Fatalf("missing committed/suppressed trace: %+v", traces)
	}
}

func TestLostRenewalResponseReconcilesExactProposal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("lost-renewal-response")
	args := append(electionCandidateArgs(key, "renew-reconciler", 4*time.Second, 3*time.Second, 1800*time.Millisecond), "--endpoint", endpoint)
	candidate, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	proxy.AddRule(faultproxy.Rule{Method: "PUT", IfMatch: true, Outcome: faultproxy.OutcomeSuppressResponse})
	output, err := candidate.Wait()
	if err != nil {
		t.Fatalf("reconciling candidate: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "reconciling unresolved lease renewal") {
		t.Fatalf("candidate did not reconcile the frozen renewal:\n%s", output)
	}
	if countEvent(candidate.Events(), "candidate_election_work_started") != 1 ||
		strings.Count(string(output), "lease release confirmed") != 1 {
		t.Fatalf("reconciliation duplicated lifecycle callbacks:\n%s", output)
	}
}

func TestLostReleaseResponseReconcilesExactProposal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("lost-release-response")
	args := append(electionCandidateArgs(key, "release-reconciler", 4*time.Second, 3*time.Second, 700*time.Millisecond), "--endpoint", endpoint)
	candidate, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	proxy.AddRule(faultproxy.Rule{
		Method: "PUT", IfMatch: true, BodyContains: `"clientID":""`, Outcome: faultproxy.OutcomeSuppressResponse,
	})
	output, err := candidate.Wait()
	if err != nil {
		t.Fatalf("release reconciliation: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "reconciling unresolved lease release") ||
		strings.Count(string(output), "lease release confirmed") != 1 {
		t.Fatalf("release was not reconciled exactly once:\n%s", output)
	}
	observation, err := newClient(t, key, "release-inspector", 4*time.Second, 3*time.Second).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ClientID != "" || observation.EpochID != 1 {
		t.Fatalf("reconciled release state = %+v", observation)
	}
}

func TestKnownElectionRecordDeletionIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("known-record-deletion")
	candidate, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "deletion-leader", 4*time.Second, 3*time.Second, 20*time.Second)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := testHarness.S3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(testHarness.Bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	output, waitErr := candidate.Wait()
	if waitErr == nil || !strings.Contains(string(output), lease.ErrLeaseDisappeared.Error()) {
		t.Fatalf("record deletion result = %v, logs:\n%s", waitErr, output)
	}
	if countEvent(candidate.Events(), "candidate_election_work_stopped") != 1 {
		t.Fatalf("record deletion did not stop work:\n%s", output)
	}
}

func TestRenewalConfirmationPastOriginalDeadlineNeverRevivesWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("late-renewal-confirmation")
	args := append(electionCandidateArgs(key, "late-renewal", 3*time.Second, 2*time.Second, 20*time.Second), "--endpoint", endpoint)
	candidate, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	proxy.AddRule(faultproxy.Rule{
		Method: "PUT", IfMatch: true, BodyContains: `"clientID":"late-renewal"`, ResponseDelay: 2500 * time.Millisecond,
	})
	proxy.AddRule(faultproxy.Rule{Method: "GET", Count: 20, Delay: 2 * time.Second})
	output, waitErr := candidate.Wait()
	if waitErr == nil || !strings.Contains(string(output), leaderelection.ErrLeadershipLost.Error()) {
		t.Fatalf("late confirmation result = %v, logs:\n%s", waitErr, output)
	}
	if countEvent(candidate.Events(), "candidate_election_work_stopped") != 1 ||
		countEvent(candidate.Events(), "candidate_election_work_completed") != 0 {
		t.Fatalf("late response revived leader work:\n%s", output)
	}
	observation, err := newClient(t, key, "late-renewal-inspector", 3*time.Second, 2*time.Second).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.EpochID != 1 || observation.SequenceID < 2 {
		t.Fatalf("delayed renewal was not committed as expected: %+v", observation)
	}
}

func TestReleaseRaceCannotClearSuccessor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	key := testHarness.Key("release-takeover-race")
	leaderArgs := append(electionCandidateArgs(key, "releasing-leader", 4*time.Second, 3*time.Second, 800*time.Millisecond), "--endpoint", endpoint)
	leader, err := testHarness.StartCandidate(ctx, leaderArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	proxy.AddRule(faultproxy.Rule{
		Method: "PUT", IfMatch: true, BodyContains: `"clientID":""`, ResponseDelay: time.Second,
	})
	follower, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "release-successor", 4*time.Second, 3*time.Second, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = leader.Wait() // Ownership loss during release reconciliation is safe in this race.
	if output, err := follower.Wait(); err != nil {
		t.Fatalf("release successor: %v\n%s", err, output)
	}
	assertElectionLifecycle(t, follower.Events(), 2)
	observation, err := newClient(t, key, "release-race-inspector", 4*time.Second, 3*time.Second).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.EpochID != 2 || observation.ClientID != "" {
		t.Fatalf("stale release changed successor state: %+v", observation)
	}
}

func TestDistinctElectionKeysRemainIsolated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := testHarness.StartCandidate(ctx, electionCandidateArgs(testHarness.Key("isolated-a"), "isolated-a", 3*time.Second, 2*time.Second, 500*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	second, err := testHarness.StartCandidate(ctx, electionCandidateArgs(testHarness.Key("isolated-b"), "isolated-b", 3*time.Second, 2*time.Second, 500*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*harness.Candidate{first, second} {
		if output, err := candidate.Wait(); err != nil {
			t.Fatalf("isolated candidate: %v\n%s", err, output)
		}
		started, ok := firstEvent(candidate.Events(), "candidate_election_work_started")
		if !ok || started.EpochID != 1 {
			t.Fatalf("isolated candidate did not independently acquire epoch 1: %+v", candidate.Events())
		}
	}
}
