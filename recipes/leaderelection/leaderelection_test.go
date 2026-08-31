package leaderelection

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

func TestRunReleasesNormalWorkWithoutWaitingForStoppedCallback(t *testing.T) {
	client := newElectionClient(t)
	workErr := errors.New("work failed")
	callbackStarted := make(chan struct{})
	finishCallback := make(chan struct{})
	elector := newTestElector(t, client, Config{
		Callbacks: Callbacks{
			OnStartedLeading: func(context.Context, uint64) error { return workErr },
			OnStoppedLeading: func() {
				close(callbackStarted)
				<-finishCallback
			},
		},
	})

	result := make(chan error, 1)
	go func() { result <- elector.Run(context.Background()) }()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("OnStoppedLeading did not run")
	}
	select {
	case err := <-result:
		if !errors.Is(err, workErr) {
			t.Fatalf("Run = %v, want work error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run waited for OnStoppedLeading")
	}
	close(finishCallback)
	if releases := client.releaseCount(); releases != 1 {
		t.Fatalf("release calls = %d, want 1", releases)
	}
	if err := elector.Run(context.Background()); !errors.Is(err, ErrRunAlreadyUsed) {
		t.Fatalf("second Run = %v, want ErrRunAlreadyUsed", err)
	}

	successor := newTestElector(t, client, Config{Callbacks: Callbacks{
		OnStartedLeading: func(context.Context, uint64) error { return nil },
	}})
	if err := successor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if epochs := client.acquiredEpochs(); len(epochs) != 2 || epochs[0] != 1 || epochs[1] != 2 {
		t.Fatalf("acquired epochs = %v, want [1 2]", epochs)
	}
}

func TestRunReportsStoppedButDoesNotReleaseWhenWorkDoesNotJoin(t *testing.T) {
	client := newElectionClient(t)
	workStarted := make(chan struct{})
	finishWork := make(chan struct{})
	stopped := make(chan struct{}, 1)
	elector := newTestElector(t, client, Config{
		ShutdownTimeout: 30 * time.Millisecond,
		ReleaseOnCancel: true,
		Callbacks: Callbacks{
			OnStartedLeading: func(context.Context, uint64) error {
				close(workStarted)
				<-finishWork
				return nil
			},
			OnStoppedLeading: func() { stopped <- struct{}{} },
		},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- elector.Run(runCtx) }()
	select {
	case <-workStarted:
	case <-time.After(time.Second):
		t.Fatal("leader work did not start")
	}
	cancel()
	if err := <-result; !errors.Is(err, ErrWorkNotStopped) {
		t.Fatalf("Run = %v, want ErrWorkNotStopped", err)
	}
	if releases := client.releaseCount(); releases != 0 {
		t.Fatalf("release calls = %d, want 0", releases)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("OnStoppedLeading was not dispatched for admitted work")
	}
	close(finishWork)
}

func newTestElector(t *testing.T, client lease.Client, overrides Config) *Elector {
	t.Helper()
	config := Config{
		Client:          client,
		RetryPeriod:     20 * time.Millisecond,
		ObserveInterval: 20 * time.Millisecond,
		ShutdownTimeout: time.Second,
		Callbacks:       overrides.Callbacks,
		ReleaseOnCancel: overrides.ReleaseOnCancel,
		Logger:          zap.NewNop(),
	}
	if overrides.ShutdownTimeout != 0 {
		config.ShutdownTimeout = overrides.ShutdownTimeout
	}
	elector, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return elector
}

type electionClient struct {
	lease.Client
	mu       xsync.RBMutex
	releases int
	epochs   []uint64
}

func newElectionClient(t *testing.T) *electionClient {
	t.Helper()
	base, err := lease.New(lease.Config{
		Store:          &electionStore{},
		Key:            lease.Key{Bucket: "bucket", ObjectKey: t.Name()},
		ClientID:       "candidate-a",
		LeaseDuration:  2 * time.Second,
		RenewDeadline:  500 * time.Millisecond,
		RequestTimeout: 100 * time.Millisecond,
		Logger:         zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &electionClient{Client: base}
}

func (c *electionClient) Require(ctx context.Context) (*lease.Lease, error) {
	acquired, err := c.Client.Require(ctx)
	if err == nil {
		c.mu.Lock()
		c.epochs = append(c.epochs, acquired.EpochID())
		c.mu.Unlock()
	}
	return acquired, err
}

func (c *electionClient) Release(ctx context.Context, acquired *lease.Lease) error {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
	return c.Client.Release(ctx, acquired)
}

func (c *electionClient) releaseCount() int {
	token := c.mu.RLock()
	defer c.mu.RUnlock(token)
	return c.releases
}

func (c *electionClient) acquiredEpochs() []uint64 {
	token := c.mu.RLock()
	defer c.mu.RUnlock(token)
	return append([]uint64(nil), c.epochs...)
}

type electionStore struct {
	mu      xsync.RBMutex
	object  *lease.StoredObject
	version uint64
}

func (s *electionStore) Get(context.Context, lease.Key) (lease.StoredObject, error) {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	if s.object == nil {
		return lease.StoredObject{}, &lease.StoreError{Operation: lease.StoreOperationGet, Kind: lease.StoreErrorNotFound}
	}
	return lease.StoredObject{Body: append([]byte(nil), s.object.Body...), Version: s.object.Version}, nil
}

func (s *electionStore) CreateIfAbsent(_ context.Context, _ lease.Key, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object != nil {
		return "", &lease.StoreError{Operation: lease.StoreOperationCreateIfAbsent, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *electionStore) CompareAndSwap(_ context.Context, _ lease.Key, expected lease.Version, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object == nil || s.object.Version != expected {
		return "", &lease.StoreError{Operation: lease.StoreOperationCAS, Kind: lease.StoreErrorPreconditionFailed}
	}
	return s.commit(body), nil
}

func (s *electionStore) commit(body []byte) lease.Version {
	s.version++
	version := lease.Version(fmt.Sprintf("etag-%d", s.version))
	s.object = &lease.StoredObject{Body: append([]byte(nil), body...), Version: version}
	return version
}
