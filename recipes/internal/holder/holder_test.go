package holder

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

func TestRunImmediatelyReconcilesAndRefreshesUnknownRenewal(t *testing.T) {
	base, err := lease.New(lease.Config{
		Store:          &holderStore{},
		Key:            lease.Key{Bucket: "bucket", ObjectKey: t.Name()},
		ClientID:       "client-a",
		LeaseDuration:  3 * time.Second,
		RenewDeadline:  2 * time.Second,
		RequestTimeout: 100 * time.Millisecond,
		Logger:         zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := base.Require(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := &renewalClient{Client: base, calls: make(chan time.Time, 3)}

	workDone := make(chan struct{})
	err = Run(context.Background(), acquired, func(context.Context, uint64) error {
		defer close(workDone)
		for range 3 {
			select {
			case <-client.calls:
			case <-time.After(2 * time.Second):
				return errors.New("timed out waiting for renewal sequence")
			}
		}
		return nil
	}, Policy{
		Client:              client,
		RetryPeriod:         300 * time.Millisecond,
		ShutdownTimeout:     time.Second,
		ReleaseOnWorkReturn: true,
		LossError:           errors.New("lease lost"),
		WorkNotStoppedError: errors.New("work not stopped"),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-workDone:
	default:
		t.Fatal("work did not join")
	}

	times := client.callTimes()
	if len(times) != 3 {
		t.Fatalf("renew calls = %d, want 3", len(times))
	}
	if delay := times[1].Sub(times[0]); delay >= 100*time.Millisecond {
		t.Fatalf("unknown renewal reconciliation waited %v, want immediate", delay)
	}
	if delay := times[2].Sub(times[1]); delay >= 100*time.Millisecond {
		t.Fatalf("fresh renewal after reconciliation waited %v, want immediate", delay)
	}
}

type renewalClient struct {
	lease.Client
	mu    xsync.RBMutex
	times []time.Time
	calls chan time.Time
}

func (c *renewalClient) Renew(ctx context.Context, acquired *lease.Lease) error {
	now := time.Now()
	c.mu.Lock()
	c.times = append(c.times, now)
	call := len(c.times)
	c.mu.Unlock()
	c.calls <- now
	switch call {
	case 1:
		return lease.ErrUnknownOutcome
	case 2:
		return nil
	default:
		return c.Client.Renew(ctx, acquired)
	}
}

func (c *renewalClient) callTimes() []time.Time {
	token := c.mu.RLock()
	defer c.mu.RUnlock(token)
	return append([]time.Time(nil), c.times...)
}

type holderStore struct {
	mu      xsync.RBMutex
	object  *lease.StoredObject
	version uint64
}

func (s *holderStore) Get(context.Context, lease.Key) (lease.StoredObject, error) {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	if s.object == nil {
		return lease.StoredObject{}, &lease.StoreError{Operation: lease.StoreOperationGet, Kind: lease.StoreErrorNotFound}
	}
	return lease.StoredObject{Body: append([]byte(nil), s.object.Body...), Version: s.object.Version}, nil
}

func (s *holderStore) CreateIfAbsent(_ context.Context, _ lease.Key, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object != nil {
		return "", &lease.StoreError{Operation: lease.StoreOperationCreateIfAbsent, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *holderStore) CompareAndSwap(_ context.Context, _ lease.Key, expected lease.Version, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object == nil || s.object.Version != expected {
		return "", &lease.StoreError{Operation: lease.StoreOperationCAS, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *holderStore) commit(body []byte) lease.Version {
	s.version++
	version := lease.Version(fmt.Sprintf("etag-%d", s.version))
	s.object = &lease.StoredObject{Body: append([]byte(nil), body...), Version: version}
	return version
}
