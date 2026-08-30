//go:build e2e

package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/samber/lo"
)

func TestCoreLifecycleAndSameClientReacquisition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("core-lifecycle")
	client := newClient(t, key, "client-a", 3*time.Second, 2*time.Second)
	acquired, err := client.Require(ctx)
	if err != nil {
		t.Fatalf("require: %v", err)
	}
	if acquired.EpochID() != 1 {
		t.Fatalf("initial epoch = %d, want 1", acquired.EpochID())
	}
	if err := acquired.Check(); err != nil {
		t.Fatalf("new lease check: %v", err)
	}
	if _, err := client.Require(ctx); !errors.Is(err, lease.ErrAlreadyHeld) {
		t.Fatalf("second local require = %v, want ErrAlreadyHeld", err)
	}
	if err := client.Renew(ctx, acquired); err != nil {
		t.Fatalf("renew: %v", err)
	}
	observation, err := client.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.EpochID != 1 || observation.SequenceID != 2 || observation.ClientID != "client-a" {
		t.Fatalf("renewed observation = %+v", observation)
	}
	if err := client.Release(ctx, acquired); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := acquired.Check(); !errors.Is(err, lease.ErrLeaseRetired) {
		t.Fatalf("released lease check = %v, want ErrLeaseRetired", err)
	}
	select {
	case <-acquired.Done():
	default:
		t.Fatal("released lease Done is open")
	}
	reacquired, err := client.Require(ctx)
	if err != nil {
		t.Fatalf("same-client reacquire: %v", err)
	}
	if reacquired == acquired || reacquired.EpochID() != 2 {
		t.Fatalf("reacquired lease=%p epoch=%d, want new lease at epoch 2", reacquired, reacquired.EpochID())
	}
	if err := client.Release(ctx, reacquired); err != nil {
		t.Fatal(err)
	}
}

func TestCoreContentionExpiryAndTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("core-expiry")
	holder := newClient(t, key, "holder", 2*time.Second, time.Second)
	follower := newClient(t, key, "follower", 2*time.Second, time.Second)
	acquired, err := holder.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := follower.Require(ctx); !errors.Is(err, lease.ErrNotEligible) {
		t.Fatalf("occupied require = %v, want ErrNotEligible", err)
	}
	select {
	case <-acquired.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("local lease did not expire")
	}
	if err := acquired.Check(); !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("expired check = %v, want ErrLeaseExpired", err)
	}
	if _, err := follower.Require(ctx); !errors.Is(err, lease.ErrNotEligible) {
		t.Fatalf("takeover at local expiry = %v, want ErrNotEligible until advertised duration", err)
	}
	// Eligibility is based on the follower's unchanged observation of the full
	// advertised duration, not the former holder's shorter local deadline.
	time.Sleep(1100 * time.Millisecond)
	taken, err := follower.Require(ctx)
	if err != nil {
		t.Fatalf("takeover after advertised duration: %v", err)
	}
	if taken.EpochID() != 2 {
		t.Fatalf("takeover epoch = %d, want 2", taken.EpochID())
	}
	if err := follower.Release(ctx, taken); err != nil {
		t.Fatal(err)
	}
}

func TestCoreConcurrentCandidatesConfirmOneHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testHarness.Key("core-contention")
	const candidates = 16
	clients := make([]lease.Client, candidates)
	for index := range candidates {
		clients[index] = newClient(t, key, "candidate-"+string(rune('a'+index)), 3*time.Second, 2*time.Second)
	}
	results := make(chan error, candidates)
	for index := range candidates {
		go func() {
			_, err := clients[index].Require(ctx)
			results <- err
		}()
	}
	errs := make([]error, 0, candidates)
	for range candidates {
		errs = append(errs, <-results)
	}
	wins := lo.CountBy(errs, func(err error) bool { return err == nil })
	knownLosses := lo.CountBy(errs, func(err error) bool {
		return errors.Is(err, lease.ErrConflict) || errors.Is(err, lease.ErrNotEligible)
	})
	if wins != 1 || knownLosses != candidates-1 {
		t.Fatalf("wins=%d known_losses=%d errors=%v", wins, knownLosses, errs)
	}
}

func TestContainerCandidateLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testHarness.Key("container-candidate")
	output, err := testHarness.RunCandidate(ctx,
		"--key", key,
		"--client-id", "container-a",
		"--lease-duration", "3s",
		"--renew-deadline", "2s",
		"--request-timeout", "500ms",
		"--renew-period", "500ms",
		"--hold-duration", "1200ms",
		"--release=true",
	)
	if err != nil {
		t.Fatal(err)
	}
	logs := string(output)
	if !strings.Contains(logs, `"msg":"candidate_acquired"`) ||
		!strings.Contains(logs, `"msg":"candidate_renewed"`) ||
		!strings.Contains(logs, `"msg":"candidate_released"`) {
		t.Fatalf("candidate lifecycle events missing:\n%s", logs)
	}
	observer := newClient(t, key, "inspector", 3*time.Second, 2*time.Second)
	observation, err := observer.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ClientID != "" || observation.EpochID != 1 || observation.SequenceID < 3 {
		t.Fatalf("candidate final observation = %+v", observation)
	}
}

func newClient(t *testing.T, objectKey, clientID string, duration, deadline time.Duration) lease.Client {
	t.Helper()
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	client, err := lease.New(lease.Config{
		Store:          store,
		Key:            lease.Key{Bucket: testHarness.Bucket, ObjectKey: objectKey},
		ClientID:       clientID,
		MetadataName:   "e2e",
		LeaseDuration:  duration,
		RenewDeadline:  deadline,
		RequestTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
