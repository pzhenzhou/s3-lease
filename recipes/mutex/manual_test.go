package mutex

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
	"go.uber.org/zap"
)

func TestWithLockSuppressesReleaseAfterFailedRenewalReconciliation(t *testing.T) {
	client := newControlledClient(t)
	secondRenewal := make(chan struct{})
	stopping := make(chan struct{})
	client.renew = func(call int, _ context.Context, _ *lease.Lease) error {
		switch call {
		case 1:
			return lease.ErrUnknownOutcome
		case 2:
			close(secondRenewal)
			<-stopping
			return lease.ErrUnavailable
		default:
			return nil
		}
	}
	metrics := testMetrics{stopping: stopping}
	mutex := newTestMutex(t, client, metrics)

	err := mutex.WithLock(context.Background(), func(context.Context, uint64) error {
		waitForSignal(t, secondRenewal, "renewal reconciliation")
		return nil
	})
	if !errors.Is(err, lease.ErrUnavailable) {
		t.Fatalf("WithLock = %v, want ErrUnavailable", err)
	}
	if renewals, releases := client.counts(); renewals != 2 || releases != 0 {
		t.Fatalf("calls after failed reconciliation: renew=%d release=%d, want renew=2 release=0", renewals, releases)
	}
}

func TestManualReleasePreservesCanceledReconciliationState(t *testing.T) {
	tests := []struct {
		name         string
		result       error
		wantErr      error
		wantRenewals int
		wantReleases int
	}{
		{
			name:         "successful reconciliation does not create renewal",
			wantRenewals: 2,
			wantReleases: 1,
		},
		{
			name:         "failed reconciliation suppresses release",
			result:       lease.ErrUnavailable,
			wantErr:      lease.ErrUnavailable,
			wantRenewals: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newControlledClient(t)
			resolving := make(chan struct{})
			client.renew = func(call int, ctx context.Context, _ *lease.Lease) error {
				switch call {
				case 1:
					return lease.ErrUnknownOutcome
				case 2:
					close(resolving)
					<-ctx.Done()
					return test.result
				default:
					return nil
				}
			}
			mutex := newTestMutex(t, client, testMetrics{})
			held, err := mutex.TryLock(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			waitForSignal(t, resolving, "renewal reconciliation")

			err = mutex.Release(context.Background(), held)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Release = %v, want %v", err, test.wantErr)
			}
			if renewals, releases := client.counts(); renewals != test.wantRenewals || releases != test.wantReleases {
				t.Fatalf("calls: renew=%d release=%d, want renew=%d release=%d",
					renewals, releases, test.wantRenewals, test.wantReleases)
			}
		})
	}
}

func TestManualReleaseSnapshotsDeadlineBeforeStoppingRenewal(t *testing.T) {
	client := newControlledClient(t)
	renewing := make(chan struct{})
	var renewedUntil time.Time
	client.renew = func(call int, ctx context.Context, acquired *lease.Lease) error {
		if call != 1 {
			return client.Client.Renew(ctx, acquired)
		}
		close(renewing)
		<-ctx.Done()
		if err := client.Client.Renew(context.Background(), acquired); err != nil {
			return err
		}
		renewedUntil = acquired.ValidUntil()
		return nil
	}
	var cleanupDeadline time.Time
	client.release = func(ctx context.Context, acquired *lease.Lease) error {
		cleanupDeadline, _ = ctx.Deadline()
		return client.Client.Release(ctx, acquired)
	}
	mutex := newTestMutex(t, client, testMetrics{})
	held, err := mutex.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	originalDeadline := held.ValidUntil()
	waitForSignal(t, renewing, "automatic renewal")

	if err := mutex.Release(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	if !renewedUntil.After(originalDeadline) {
		t.Fatalf("renewed deadline = %v, want after original %v", renewedUntil, originalDeadline)
	}
	if !cleanupDeadline.Equal(originalDeadline) {
		t.Fatalf("cleanup deadline = %v, want original authority deadline %v", cleanupDeadline, originalDeadline)
	}
}

func TestManualReleaseCancellationRetiresHandleAndEventuallyClearsOwnership(t *testing.T) {
	client := newControlledClient(t)
	renewing := make(chan struct{})
	finishRenewal := make(chan struct{})
	client.renew = func(_ int, _ context.Context, _ *lease.Lease) error {
		close(renewing)
		<-finishRenewal
		return nil
	}
	mutex := newTestMutex(t, client, testMetrics{})
	held, err := mutex.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, renewing, "automatic renewal")

	releaseCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := mutex.Release(releaseCtx, held); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Release = %v, want context deadline exceeded", err)
	}
	select {
	case <-held.Done():
	default:
		t.Fatal("Lock.Done remained open after release canceled renewal")
	}
	if _, err := mutex.TryLock(context.Background()); !errors.Is(err, ErrRecipeBusy) {
		t.Fatalf("TryLock before underlying expiry = %v, want ErrRecipeBusy", err)
	}
	close(finishRenewal)
	waitForSignal(t, held.acquired.Done(), "underlying lease expiry")

	deadline := time.Now().Add(time.Second)
	for {
		_, err := mutex.TryLock(context.Background())
		if !errors.Is(err, ErrRecipeBusy) {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("TryLock after underlying expiry remained locally busy: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAutomaticManualLeaseLossClearsOwnership(t *testing.T) {
	client := newControlledClient(t)
	client.renew = func(_ int, _ context.Context, acquired *lease.Lease) error {
		if err := client.Client.Release(context.Background(), acquired); err != nil {
			return err
		}
		return acquired.Check()
	}
	mutex := newTestMutex(t, client, testMetrics{})
	held, err := mutex.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, held.Done(), "automatic lease loss")

	reacquired, err := mutex.TryLock(context.Background())
	if err != nil {
		t.Fatalf("TryLock after automatic loss = %v", err)
	}
	if reacquired.EpochID() != held.EpochID()+1 {
		t.Fatalf("reacquired epoch = %d, want %d", reacquired.EpochID(), held.EpochID()+1)
	}
	if err := mutex.Release(context.Background(), reacquired); err != nil {
		t.Fatal(err)
	}
}

type testMetrics struct {
	stopping chan struct{}
}

func (m testMetrics) LockChanged(held bool, _ uint64) {
	if held || m.stopping == nil {
		return
	}
	select {
	case <-m.stopping:
	default:
		close(m.stopping)
	}
}

func (testMetrics) WorkShutdown(time.Duration, bool) {}

type controlledClient struct {
	lease.Client

	mu           xsync.RBMutex
	renewCalls   int
	releaseCalls int
	renew        func(int, context.Context, *lease.Lease) error
	release      func(context.Context, *lease.Lease) error
}

func newControlledClient(t *testing.T) *controlledClient {
	t.Helper()
	base, err := lease.New(lease.Config{
		Store:          &recipeStore{},
		Key:            lease.Key{Bucket: "bucket", ObjectKey: t.Name()},
		ClientID:       "client-a",
		LeaseDuration:  2 * time.Second,
		RenewDeadline:  500 * time.Millisecond,
		RequestTimeout: 100 * time.Millisecond,
		Logger:         zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &controlledClient{Client: base}
}

func (c *controlledClient) Renew(ctx context.Context, acquired *lease.Lease) error {
	c.mu.Lock()
	c.renewCalls++
	call := c.renewCalls
	hook := c.renew
	c.mu.Unlock()
	if hook != nil {
		return hook(call, ctx, acquired)
	}
	return c.Client.Renew(ctx, acquired)
}

func (c *controlledClient) Release(ctx context.Context, acquired *lease.Lease) error {
	c.mu.Lock()
	c.releaseCalls++
	hook := c.release
	c.mu.Unlock()
	if hook != nil {
		return hook(ctx, acquired)
	}
	return c.Client.Release(ctx, acquired)
}

func (c *controlledClient) counts() (renewals, releases int) {
	token := c.mu.RLock()
	defer c.mu.RUnlock(token)
	return c.renewCalls, c.releaseCalls
}

type recipeStore struct {
	mu      xsync.RBMutex
	object  *lease.StoredObject
	version uint64
}

func (s *recipeStore) Get(context.Context, lease.Key) (lease.StoredObject, error) {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	if s.object == nil {
		return lease.StoredObject{}, &lease.StoreError{Operation: lease.StoreOperationGet, Kind: lease.StoreErrorNotFound}
	}
	return lease.StoredObject{Body: append([]byte(nil), s.object.Body...), Version: s.object.Version}, nil
}

func (s *recipeStore) CreateIfAbsent(_ context.Context, _ lease.Key, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object != nil {
		return "", &lease.StoreError{Operation: lease.StoreOperationCreateIfAbsent, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *recipeStore) CompareAndSwap(_ context.Context, _ lease.Key, expected lease.Version, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object == nil || s.object.Version != expected {
		return "", &lease.StoreError{Operation: lease.StoreOperationCAS, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *recipeStore) commit(body []byte) lease.Version {
	s.version++
	version := lease.Version(fmt.Sprintf("etag-%d", s.version))
	s.object = &lease.StoredObject{Body: append([]byte(nil), body...), Version: version}
	return version
}

func newTestMutex(t *testing.T, client lease.Client, metrics Metrics) *Mutex {
	t.Helper()
	mutex, err := New(Config{
		Client:          client,
		RetryPeriod:     20 * time.Millisecond,
		ObserveInterval: 20 * time.Millisecond,
		ShutdownTimeout: time.Second,
		Metrics:         metrics,
		Logger:          zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return mutex
}

func waitForSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
