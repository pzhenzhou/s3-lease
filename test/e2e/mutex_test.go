//go:build e2e

package e2e

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/mutex"
	"github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"
)

type candidateEvent struct {
	Timestamp float64 `json:"ts"`
	Message   string  `json:"msg"`
	ClientID  string  `json:"client_id"`
	EpochID   uint64  `json:"epoch_id"`
}

type workInterval struct {
	ClientID string
	EpochID  uint64
	Started  float64
	Ended    float64
}

func TestMutexContendersRunSeriallyAndRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("mutex-contention")
	const candidates = 3

	running := make([]*harness.Candidate, 0, candidates)
	clientIDs := []string{"mutex-a", "mutex-b", "mutex-c"}
	first, err := testHarness.StartCandidate(ctx, mutexCandidateArgs(key, clientIDs[0], 4*time.Second, 3*time.Second, 500*time.Millisecond, 1500*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	running = append(running, first)
	observer := newClient(t, key, "mutex-inspector", 4*time.Second, 3*time.Second)
	waitForOwner(t, ctx, observer, clientIDs[0])

	for _, clientID := range clientIDs[1:] {
		candidate, startErr := testHarness.StartCandidate(ctx, mutexCandidateArgs(key, clientID, 4*time.Second, 3*time.Second, 500*time.Millisecond, 500*time.Millisecond)...)
		if startErr != nil {
			t.Fatal(startErr)
		}
		running = append(running, candidate)
	}

	outputs := make([][]byte, 0, candidates)
	for index, candidate := range running {
		output, waitErr := candidate.Wait()
		if waitErr != nil {
			t.Fatalf("candidate %s: %v", clientIDs[index], waitErr)
		}
		outputs = append(outputs, output)
	}

	intervals := make([]workInterval, 0, candidates)
	for index, output := range outputs {
		events := decodeCandidateEvents(t, output)
		intervals = append(intervals, intervalFor(t, clientIDs[index], events))
		if countMessage(events, "lease release confirmed") != 1 {
			t.Fatalf("candidate %s did not confirm exactly one active release:\n%s", clientIDs[index], output)
		}
	}
	slices.SortFunc(intervals, func(left, right workInterval) int {
		if left.EpochID < right.EpochID {
			return -1
		}
		if left.EpochID > right.EpochID {
			return 1
		}
		return 0
	})
	for index, interval := range intervals {
		wantEpoch := uint64(index + 1)
		if interval.EpochID != wantEpoch {
			t.Fatalf("ordered interval %d has epoch %d, want %d: %+v", index, interval.EpochID, wantEpoch, intervals)
		}
		if index > 0 && interval.Started < intervals[index-1].Ended {
			t.Fatalf("protected work overlapped: %+v", intervals)
		}
	}
	observation, err := observer.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ClientID != "" || observation.EpochID != candidates {
		t.Fatalf("final mutex observation = %+v, want released epoch %d", observation, candidates)
	}
}

func TestMutexTakeoverAfterUnreleasedLeaseExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("mutex-expiry")
	leaseDuration := 3 * time.Second
	observer := newClient(t, key, "expiry-inspector", leaseDuration, 2*time.Second)

	holderArgs := mutexCandidateArgs(key, "timeout-holder", leaseDuration, 2*time.Second, time.Second, 10*time.Second)
	holderArgs = append(holderArgs, "--cancel-after", "500ms", "--release-on-cancel=false")
	holder, err := testHarness.StartCandidate(ctx, holderArgs...)
	if err != nil {
		t.Fatal(err)
	}
	waitForOwner(t, ctx, observer, "timeout-holder")
	holderOutput, holderErr := holder.Wait()
	if holderErr == nil || !strings.Contains(string(holderOutput), "context canceled") {
		t.Fatalf("canceled holder result = %v, logs:\n%s", holderErr, holderOutput)
	}
	holderEvents := decodeCandidateEvents(t, holderOutput)
	if countMessage(holderEvents, "candidate_mutex_work_started") != 1 ||
		countMessage(holderEvents, "candidate_mutex_work_stopped") != 1 {
		t.Fatalf("holder work lifecycle missing:\n%s", holderOutput)
	}
	if countMessage(holderEvents, "lease release confirmed") != 0 {
		t.Fatalf("canceled holder actively released despite ReleaseOnCancel=false:\n%s", holderOutput)
	}

	stillHeld, err := observer.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stillHeld.ClientID != "timeout-holder" || stillHeld.EpochID != 1 {
		t.Fatalf("record after canceled holder = %+v, want occupied epoch 1", stillHeld)
	}

	followerStarted := time.Now()
	followerOutput, err := testHarness.RunCandidate(ctx, mutexCandidateArgs(key, "timeout-follower", leaseDuration, 2*time.Second, 400*time.Millisecond, 300*time.Millisecond)...)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(followerStarted); elapsed < leaseDuration {
		t.Fatalf("follower completed in %v, before the %v unchanged-record timeout", elapsed, leaseDuration)
	}
	followerEvents := decodeCandidateEvents(t, followerOutput)
	interval := intervalFor(t, "timeout-follower", followerEvents)
	if interval.EpochID != 2 || countMessage(followerEvents, "lease release confirmed") != 1 {
		t.Fatalf("follower lifecycle = %+v, logs:\n%s", interval, followerOutput)
	}
	final, err := observer.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if final.ClientID != "" || final.EpochID != 2 {
		t.Fatalf("final timeout observation = %+v, want released epoch 2", final)
	}
}

func TestMutexTryLockReturnsImmediatelyAndManualRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := testHarness.Key("mutex-try-lock")
	firstClient := newClient(t, key, "try-lock-a", 3*time.Second, 2*time.Second)
	secondClient := newClient(t, key, "try-lock-b", 3*time.Second, 2*time.Second)
	first := newMutex(t, firstClient, 400*time.Millisecond)
	second := newMutex(t, secondClient, 400*time.Millisecond)

	held, err := first.TryLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if held.EpochID() != 1 || held.Check() != nil {
		t.Fatalf("first manual lock epoch=%d check=%v", held.EpochID(), held.Check())
	}
	if _, err := first.TryLock(ctx); !errors.Is(err, mutex.ErrRecipeBusy) {
		t.Fatalf("overlapping local TryLock = %v, want ErrRecipeBusy", err)
	}

	attemptStarted := time.Now()
	if _, err := second.TryLock(ctx); !errors.Is(err, lease.ErrNotEligible) {
		t.Fatalf("contended TryLock = %v, want immediate ErrNotEligible", err)
	}
	if elapsed := time.Since(attemptStarted); elapsed >= time.Second {
		t.Fatalf("contended TryLock blocked for %v", elapsed)
	}

	select {
	case <-held.Done():
		t.Fatal("manual lock was lost before its renewal window completed")
	case <-time.After(2300 * time.Millisecond):
	}
	if err := held.Check(); err != nil {
		t.Fatalf("automatically renewed manual lock: %v", err)
	}
	if err := first.Release(ctx, held); err != nil {
		t.Fatalf("manual release: %v", err)
	}
	select {
	case <-held.Done():
	default:
		t.Fatal("released manual lock Done is open")
	}
	if err := held.Check(); !errors.Is(err, lease.ErrLeaseRetired) {
		t.Fatalf("released manual lock check = %v, want ErrLeaseRetired", err)
	}

	reacquired, err := second.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock after active release: %v", err)
	}
	if reacquired.EpochID() != 2 {
		t.Fatalf("reacquired epoch = %d, want 2", reacquired.EpochID())
	}
	if err := second.Release(ctx, reacquired); err != nil {
		t.Fatal(err)
	}
}

func mutexCandidateArgs(key, clientID string, duration, deadline, retry, hold time.Duration) []string {
	return []string{
		"--mode", "mutex",
		"--key", key,
		"--client-id", clientID,
		"--lease-duration", duration.String(),
		"--renew-deadline", deadline.String(),
		"--request-timeout", "300ms",
		"--retry-period", retry.String(),
		"--observe-interval", "200ms",
		"--shutdown-timeout", "1s",
		"--hold-duration", hold.String(),
	}
}

func newMutex(t *testing.T, client lease.Client, retry time.Duration) *mutex.Mutex {
	t.Helper()
	lock, err := mutex.New(mutex.Config{
		Client:          client,
		RetryPeriod:     retry,
		ObserveInterval: 200 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func waitForOwner(t *testing.T, ctx context.Context, client lease.Client, owner string) lease.Observation {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		observation, err := client.Observe(ctx)
		if err == nil && observation.ClientID == owner {
			return observation
		}
		if err != nil && !errors.Is(err, lease.ErrNotFound) && !errors.Is(err, lease.ErrUnavailable) {
			t.Fatalf("observe owner %s: %v", owner, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for owner %s: %v", owner, ctx.Err())
		case <-ticker.C:
		}
	}
}

func decodeCandidateEvents(t *testing.T, output []byte) []candidateEvent {
	t.Helper()
	events := make([]candidateEvent, 0)
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		var event candidateEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}
		events = append(events, event)
	}
	if len(events) == 0 {
		t.Fatalf("candidate emitted no structured events:\n%s", output)
	}
	return events
}

func intervalFor(t *testing.T, clientID string, events []candidateEvent) workInterval {
	t.Helper()
	var interval workInterval
	for _, event := range events {
		switch event.Message {
		case "candidate_mutex_work_started":
			if interval.Started != 0 {
				t.Fatalf("client %s started work more than once", clientID)
			}
			interval = workInterval{ClientID: event.ClientID, EpochID: event.EpochID, Started: event.Timestamp}
		case "candidate_mutex_work_completed", "candidate_mutex_work_stopped":
			if interval.Ended != 0 {
				t.Fatalf("client %s stopped work more than once", clientID)
			}
			interval.Ended = event.Timestamp
		}
	}
	if interval.ClientID != clientID || interval.EpochID == 0 || interval.Started == 0 || interval.Ended == 0 {
		t.Fatalf("incomplete work interval for %s: %+v", clientID, interval)
	}
	return interval
}

func countMessage(events []candidateEvent, message string) int {
	count := 0
	for _, event := range events {
		if event.Message == message {
			count++
		}
	}
	return count
}
