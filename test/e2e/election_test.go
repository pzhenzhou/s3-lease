//go:build e2e

package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/recipes/leaderelection"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/faultproxy"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"
)

func TestElectionLifecycleReleaseAndSameIDSuccession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("election-lifecycle")

	firstArgs := append(electionCandidateArgs(key, "same-id", 3*time.Second, 2*time.Second, 700*time.Millisecond), "--release-on-cancel=false")
	first, err := testHarness.StartCandidate(ctx, firstArgs...)
	if err != nil {
		t.Fatal(err)
	}
	firstOutput, err := first.Wait()
	if err != nil {
		t.Fatal(err)
	}
	firstEvents := first.Events()
	assertElectionLifecycle(t, firstEvents, 1)
	if !strings.Contains(string(firstOutput), "lease release confirmed") {
		t.Fatalf("first election did not release:\n%s", firstOutput)
	}

	second, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "same-id", 3*time.Second, 2*time.Second, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	secondOutput, err := second.Wait()
	if err != nil {
		t.Fatal(err)
	}
	assertElectionLifecycle(t, second.Events(), 2)
	if !strings.Contains(string(secondOutput), "lease release confirmed") {
		t.Fatalf("successor election did not release:\n%s", secondOutput)
	}
}

func TestElectionCrashTakeoverWaitsForFullObservationInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("election-crash-takeover")
	duration := 3 * time.Second
	leader, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "crashed-leader", duration, 2*time.Second, 20*time.Second)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = leader.Wait()

	started := time.Now()
	follower, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "crash-follower", duration, 2*time.Second, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := follower.Wait(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < duration-150*time.Millisecond {
		t.Fatalf("takeover completed in %v, before a full %v unchanged observation", elapsed, duration)
	}
	assertElectionLifecycle(t, follower.Events(), 2)
}

func TestConcurrentElectionCandidatesConfirmOneInitialLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("election-contention")
	candidates := make([]*harness.Candidate, 0, 3)
	for _, clientID := range []string{"candidate-a", "candidate-b", "candidate-c"} {
		candidate, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, clientID, 3*time.Second, 2*time.Second, 2*time.Second)...)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	for _, candidate := range candidates {
		if output, err := candidate.Wait(); err != nil {
			t.Fatalf("contender %s: %v\n%s", candidate.Name, err, output)
		}
	}
	initialLeaders := 0
	initialLeaderID := ""
	for _, candidate := range candidates {
		for _, event := range candidate.Events() {
			if event.Message == "candidate_election_work_started" && event.EpochID == 1 {
				initialLeaders++
				initialLeaderID = event.ClientID
			}
		}
	}
	if initialLeaders != 1 {
		t.Fatalf("confirmed initial leaders = %d, want exactly one", initialLeaders)
	}
	for _, candidate := range candidates {
		candidateID := ""
		for _, event := range candidate.Events() {
			if event.Message == "candidate_election_work_started" {
				candidateID = event.ClientID
				break
			}
		}
		if candidateID == initialLeaderID {
			continue
		}
		observed := false
		for _, event := range candidate.Events() {
			if event.Message == "candidate_election_observer_started" && event.EpochID == 1 &&
				event.ObservedClientID == initialLeaderID {
				observed = true
				break
			}
		}
		if !observed {
			t.Fatalf("candidate %s did not observe initial leader %s: %+v", candidate.Name, initialLeaderID, candidate.Events())
		}
	}
}

func TestElectionNonCooperativeWorkSuppressesRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := testHarness.Key("election-noncooperative")
	args := electionCandidateArgs(key, "stuck-leader", 4*time.Second, 3*time.Second, 5*time.Second)
	args = append(args, "--cancel-after", "300ms", "--shutdown-timeout", "400ms", "--work-behavior", "noncooperative")
	candidate, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	output, waitErr := candidate.Wait()
	if waitErr == nil || !strings.Contains(string(output), leaderelection.ErrWorkNotStopped.Error()) {
		t.Fatalf("noncooperative result = %v, logs:\n%s", waitErr, output)
	}
	if strings.Contains(string(output), "lease release confirmed") {
		t.Fatalf("noncooperative leader released:\n%s", output)
	}
	observation := waitForOwner(t, ctx, newClient(t, key, "stuck-inspector", 4*time.Second, 3*time.Second), "stuck-leader")
	if observation.EpochID != 1 {
		t.Fatalf("stuck leader observation = %+v", observation)
	}
}

func TestElectionRunIsSingleUseAndCancellationJoinsWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := newClient(t, testHarness.Key("election-single-use"), "single-use", 3*time.Second, 2*time.Second)
	workStarted := make(chan struct{})
	elector, err := leaderelection.New(leaderelection.Config{
		Client: client, RetryPeriod: 400 * time.Millisecond, ObserveInterval: 200 * time.Millisecond,
		ShutdownTimeout: time.Second,
		Callbacks: leaderelection.Callbacks{OnStartedLeading: func(workCtx context.Context, _ uint64) error {
			close(workStarted)
			<-workCtx.Done()
			return workCtx.Err()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- elector.Run(runCtx) }()
	select {
	case <-workStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := elector.Run(ctx); !errors.Is(err, leaderelection.ErrRunAlreadyUsed) {
		t.Fatalf("concurrent Run = %v, want ErrRunAlreadyUsed", err)
	}
	stop()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run = %v, want context cancellation", err)
	}
	if err := elector.Run(ctx); !errors.Is(err, leaderelection.ErrRunAlreadyUsed) {
		t.Fatalf("repeated Run = %v, want ErrRunAlreadyUsed", err)
	}
}

func TestElectionWorkReturnCancellationRaceStopsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := newClient(t, testHarness.Key("election-work-cancel-race"), "work-cancel-race", 3*time.Second, 2*time.Second)
	workStarted := make(chan struct{})
	finishWork := make(chan struct{})
	stopped := make(chan struct{}, 2)
	elector, err := leaderelection.New(leaderelection.Config{
		Client: client, RetryPeriod: 400 * time.Millisecond, ObserveInterval: 200 * time.Millisecond,
		ShutdownTimeout: time.Second, ReleaseOnCancel: true,
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(workCtx context.Context, _ uint64) error {
				close(workStarted)
				select {
				case <-finishWork:
					return nil
				case <-workCtx.Done():
					return workCtx.Err()
				}
			},
			OnStoppedLeading: func() { stopped <- struct{}{} },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- elector.Run(runCtx) }()
	select {
	case <-workStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go close(finishWork)
	go stopRun()
	runErr := <-result
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("work/cancellation race returned %v", runErr)
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("OnStoppedLeading was not dispatched")
	}
	if len(stopped) != 0 {
		t.Fatal("OnStoppedLeading was dispatched more than once")
	}
}

func TestElectionAndMutexContendThroughSameKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("election-mutex-contention")
	leader, err := testHarness.StartCandidate(ctx, electionCandidateArgs(key, "election-owner", 3*time.Second, 2*time.Second, 800*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.WaitForEvent(ctx, func(event harness.Event) bool {
		return event.Message == "candidate_election_work_started"
	}); err != nil {
		t.Fatal(err)
	}
	mutexCandidate, err := testHarness.StartCandidate(ctx, mutexCandidateArgs(key, "mutex-successor", 3*time.Second, 2*time.Second, 300*time.Millisecond, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Wait(); err != nil {
		t.Fatal(err)
	}
	if output, err := mutexCandidate.Wait(); err != nil {
		t.Fatalf("mutex successor: %v\n%s", err, output)
	}
	interval := intervalFor(t, "mutex-successor", decodeCandidateEvents(t, mutexCandidate.Output()))
	if interval.EpochID != 2 {
		t.Fatalf("mutex successor epoch = %d, want 2", interval.EpochID)
	}
}

func TestSlowObserverCoalescesPendingTransitionsInLocalOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("slow-observer")
	firstClient := newClient(t, key, "observed-a", 10*time.Second, 8*time.Second)
	first, err := firstClient.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := faultproxy.New(testHarness.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close(context.Background())
	endpoint, err := testHarness.ContainerEndpoint(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	args := electionCandidateArgs(key, "slow-observer", 10*time.Second, 8*time.Second, 10*time.Second)
	args = append(args, "--retry-period", "5s", "--observe-interval", "50ms", "--observer-delay", "800ms", "--endpoint", endpoint)
	observer, err := testHarness.StartCandidate(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observer.WaitForEvent(ctx, observedCallbackStarted("observed-a")); err != nil {
		t.Fatal(err)
	}
	if err := firstClient.Release(ctx, first); err != nil {
		t.Fatal(err)
	}

	secondClient := newClient(t, key, "observed-b", 10*time.Second, 8*time.Second)
	second, err := secondClient.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reads := countProxyMethod(proxy.Traces(), "GET")
	waitForProxyMethodCount(t, ctx, proxy, "GET", reads+1)
	if err := secondClient.Release(ctx, second); err != nil {
		t.Fatal(err)
	}

	thirdClient := newClient(t, key, "observed-c", 10*time.Second, 8*time.Second)
	third, err := thirdClient.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reads = countProxyMethod(proxy.Traces(), "GET")
	waitForProxyMethodCount(t, ctx, proxy, "GET", reads+1)
	if _, err := observer.WaitForEvent(ctx, observedCallbackStarted("observed-c")); err != nil {
		t.Fatal(err)
	}
	if countObservedStart(observer.Events(), "observed-b") != 0 {
		t.Fatalf("replaceable pending observation was not coalesced: %+v", observer.Events())
	}
	if err := observer.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = observer.Wait()
	if err := thirdClient.Release(ctx, third); err != nil {
		t.Fatal(err)
	}
}

func observedCallbackStarted(clientID string) func(harness.Event) bool {
	return func(event harness.Event) bool {
		return event.Message == "candidate_election_observer_started" && event.ObservedClientID == clientID
	}
}

func countObservedStart(events []harness.Event, clientID string) int {
	count := 0
	for _, event := range events {
		if observedCallbackStarted(clientID)(event) {
			count++
		}
	}
	return count
}

func countProxyMethod(traces []faultproxy.Trace, method string) int {
	count := 0
	for _, trace := range traces {
		if trace.Method == method {
			count++
		}
	}
	return count
}

func waitForProxyMethodCount(t *testing.T, ctx context.Context, proxy *faultproxy.Proxy, method string, want int) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if countProxyMethod(proxy.Traces(), method) >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %d %s proxy requests: %v; traces=%+v", want, method, ctx.Err(), proxy.Traces())
		case <-ticker.C:
		}
	}
}

func electionCandidateArgs(key, clientID string, duration, deadline, hold time.Duration) []string {
	return []string{
		"--mode", "election", "--key", key, "--client-id", clientID,
		"--lease-duration", duration.String(), "--renew-deadline", deadline.String(),
		"--request-timeout", "300ms", "--retry-period", "400ms", "--observe-interval", "150ms",
		"--shutdown-timeout", "1s", "--hold-duration", hold.String(), "--release-on-cancel=true",
	}
}

func assertElectionLifecycle(t *testing.T, events []harness.Event, epochID uint64) {
	t.Helper()
	started, ok := firstEvent(events, "candidate_election_work_started")
	if !ok || started.EpochID != epochID {
		t.Fatalf("work start = %+v, found=%v, want epoch %d; events=%+v", started, ok, epochID, events)
	}
	if countEvent(events, "candidate_election_work_started") != 1 ||
		countEvent(events, "candidate_election_stopped_leading") != 1 {
		t.Fatalf("election callbacks not exactly once: %+v", events)
	}
}
