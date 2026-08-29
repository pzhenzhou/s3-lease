package lease

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

func TestLeaseLifecycleAndUnknownRenewalReconciliation(t *testing.T) {
	store := &memoryStore{}
	client := newTestClient(t, store, "client-a")
	ctx := context.Background()
	acquired, err := client.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.EpochID() != 1 || acquired.Check() != nil {
		t.Fatalf("initial lease: epoch=%d check=%v", acquired.EpochID(), acquired.Check())
	}

	store.setAmbiguousNext()
	renewStarted := time.Now()
	if err := client.Renew(ctx, acquired); !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("ambiguous renewal = %v, want ErrUnknownOutcome", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := client.Renew(ctx, acquired); err != nil {
		t.Fatalf("renewal readback reconciliation: %v", err)
	}
	if deadline := acquired.ValidUntil(); deadline.After(renewStarted.Add(350 * time.Millisecond)) {
		t.Fatalf("reconciliation moved fixed deadline to %v", deadline)
	}
	if err := client.Release(ctx, acquired); err != nil {
		t.Fatal(err)
	}
	if err := acquired.Check(); !errors.Is(err, ErrLeaseRetired) {
		t.Fatalf("released lease = %v, want ErrLeaseRetired", err)
	}
	reacquired, err := client.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reacquired == acquired || reacquired.EpochID() != 2 {
		t.Fatalf("reacquired lease=%p epoch=%d", reacquired, reacquired.EpochID())
	}
}

func TestRenewalReconcilesPreconditionFailureAfterCommit(t *testing.T) {
	store := &memoryStore{}
	client := newTestClient(t, store, "client-a")
	acquired, err := client.Require(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	store.setPreconditionAfterCommitNext()
	if err := client.Renew(context.Background(), acquired); err != nil {
		t.Fatalf("Renew = %v, want exact readback confirmation", err)
	}
	if acquired.sequenceID != 2 {
		t.Fatalf("sequence = %d, want 2", acquired.sequenceID)
	}
	if err := acquired.Check(); err != nil {
		t.Fatalf("confirmed lease Check = %v", err)
	}
}

func TestObserveDiscardsSupersededReadCompletion(t *testing.T) {
	config := testConfig(&memoryStore{}, "client-a")
	first, err := newInitialRecord(config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := renewalRecord(first, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	firstBody, err := encodeRecord(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBody, err := encodeRecord(second)
	if err != nil {
		t.Fatal(err)
	}
	store := &orderedReadStore{
		objects: []StoredObject{{Body: firstBody, Version: "etag-1"}, {Body: secondBody, Version: "etag-2"}},
		entered: []chan struct{}{make(chan struct{}), make(chan struct{})},
		release: make(chan struct{}),
	}
	config.Store = store
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan Observation, 2)
	errs := make(chan error, 2)
	go func() {
		observation, observeErr := client.Observe(context.Background())
		results <- observation
		errs <- observeErr
	}()
	<-store.entered[0]
	go func() {
		observation, observeErr := client.Observe(context.Background())
		results <- observation
		errs <- observeErr
	}()
	<-store.entered[1]
	if err := <-errs; err != nil {
		t.Fatalf("newer Observe = %v", err)
	}
	newer := <-results
	if newer.SequenceID != 2 {
		t.Fatalf("newer sequence = %d, want 2", newer.SequenceID)
	}
	close(store.release)
	if err := <-errs; err != nil {
		t.Fatalf("superseded Observe = %v", err)
	}
	if superseded := <-results; superseded.SequenceID != 2 {
		t.Fatalf("superseded completion returned sequence %d, want latest sequence 2", superseded.SequenceID)
	}
}

func TestRequireAbandonsExpiredRenewalProposal(t *testing.T) {
	store := &memoryStore{}
	clock := newPausedClock(time.Now())
	client, err := New(testConfigWithClock(store, "client-a", clock))
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := client.Require(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store.setAmbiguousNext()
	if err := client.Renew(context.Background(), acquired); !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("Renew = %v, want ErrUnknownOutcome", err)
	}

	clock.Advance(300 * time.Millisecond)
	if _, err := client.Require(context.Background()); !errors.Is(err, ErrNotEligible) {
		t.Fatalf("Require after proposal expiry = %v, want ErrNotEligible", err)
	}
	clock.Advance(time.Second)
	reacquired, err := client.Require(context.Background())
	if err != nil {
		t.Fatalf("Require after advertised expiry = %v", err)
	}
	if reacquired.EpochID() != 2 {
		t.Fatalf("reacquired epoch = %d, want 2", reacquired.EpochID())
	}
}

func TestAcquisitionConfirmationRechecksDeadlineUnderStateLock(t *testing.T) {
	clock := newPausedClock(time.Now())
	client, err := New(testConfigWithClock(&memoryStore{}, "client-a", clock))
	if err != nil {
		t.Fatal(err)
	}
	c := client.(*core)
	record, err := newInitialRecord(c.config, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	body, err := encodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	acquisition := &proposal{kind: proposalAcquire, record: record, body: body, firstSendAt: clock.Now()}

	c.stateMu.Lock()
	result := make(chan error, 1)
	go func() {
		_, confirmErr := c.confirmAcquisition(context.Background(), acquisition, "etag-1")
		result <- confirmErr
	}()
	clock.Advance(c.config.RenewDeadline)
	c.stateMu.Unlock()
	if err := <-result; !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("confirmation = %v, want ErrUnknownOutcome", err)
	}
	if c.active != nil {
		t.Fatal("expired acquisition installed an active lease")
	}
}

func TestNewRejectsTypedNilClock(t *testing.T) {
	var clock *nilClock
	_, err := New(testConfigWithClock(&memoryStore{}, "client-a", clock))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New = %v, want ErrInvalidConfig", err)
	}
}

func TestLeaseExpiresAndCannotRevive(t *testing.T) {
	store := &memoryStore{}
	client := newTestClient(t, store, "client-a")
	acquired, err := client.Require(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close at the authority deadline")
	}
	if err := acquired.Check(); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Check = %v, want ErrLeaseExpired", err)
	}
	if err := client.Renew(context.Background(), acquired); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late Renew = %v, want ErrLeaseExpired", err)
	}
}

func TestClientRejectsConcurrentMutation(t *testing.T) {
	base := &memoryStore{}
	store := &blockingStore{memoryStore: base, entered: make(chan struct{}), proceed: make(chan struct{})}
	client := newTestClient(t, store, "client-a")
	acquired, err := client.Require(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- client.Renew(context.Background(), acquired) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("renewal did not reach CAS")
	}
	if err := client.Release(context.Background(), acquired); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("overlapping Release = %v, want ErrConcurrentMutation", err)
	}
	close(store.proceed)
	if err := <-result; err != nil {
		t.Fatalf("renewal after unblock: %v", err)
	}
}

func TestClientTreatsMalformedStateAndDisappearanceAsTerminal(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		store := &memoryStore{object: &StoredObject{
			Body:    []byte(`{"apiVersion":"coordination.pactdata.io/v1alpha1","kind":"Lease","metadata":{"uid":"u","createdAt":"2026-01-01T00:00:00Z"},"spec":{"clientID":"x","leaseDurationSeconds":1,"acquireTime":"2026-01-01T00:00:00Z","renewTime":"2026-01-01T00:00:00Z","epochID":1,"sequenceID":1},"extra":true}`),
			Version: "etag-1",
		}}
		client := newTestClient(t, store, "client-a")
		if _, err := client.Observe(context.Background()); !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("Observe = %v, want ErrProtocolViolation", err)
		}
		if _, err := client.Observe(context.Background()); !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("terminal Observe = %v, want ErrProtocolViolation", err)
		}
	})

	t.Run("known object disappears", func(t *testing.T) {
		store := &memoryStore{}
		client := newTestClient(t, store, "client-a")
		acquired, err := client.Require(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		store.remove()
		if _, err := client.Observe(context.Background()); !errors.Is(err, ErrLeaseDisappeared) {
			t.Fatalf("Observe = %v, want ErrLeaseDisappeared", err)
		}
		if err := acquired.Check(); !errors.Is(err, ErrOwnershipLost) {
			t.Fatalf("lease after disappearance = %v, want ErrOwnershipLost", err)
		}
	})
}

func newTestClient(t *testing.T, store LeaseStore, clientID string) Client {
	t.Helper()
	client, err := New(testConfig(store, clientID))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testConfig(store LeaseStore, clientID string) Config {
	return Config{
		Store:          store,
		Key:            Key{Bucket: "bucket", ObjectKey: "lease.json"},
		ClientID:       clientID,
		LeaseDuration:  time.Second,
		RenewDeadline:  300 * time.Millisecond,
		RequestTimeout: 50 * time.Millisecond,
	}
}

func testConfigWithClock(store LeaseStore, clientID string, clock Clock) Config {
	config := testConfig(store, clientID)
	config.Clock = clock
	return config
}

type memoryStore struct {
	mu                          xsync.RBMutex
	object                      *StoredObject
	version                     uint64
	ambiguousNext               bool
	preconditionAfterCommitNext bool
}

func (s *memoryStore) Get(_ context.Context, _ Key) (StoredObject, error) {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	if s.object == nil {
		return StoredObject{}, &StoreError{Operation: StoreOperationGet, Kind: StoreErrorNotFound}
	}
	return StoredObject{Body: append([]byte(nil), s.object.Body...), Version: s.object.Version}, nil
}

func (s *memoryStore) CreateIfAbsent(_ context.Context, _ Key, body []byte) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object != nil {
		return "", &StoreError{Operation: StoreOperationCreateIfAbsent, Kind: StoreErrorPreconditionFailed}
	}
	return s.commitLocked(body)
}

func (s *memoryStore) CompareAndSwap(_ context.Context, _ Key, expected Version, body []byte) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object == nil || s.object.Version != expected {
		return "", &StoreError{Operation: StoreOperationCAS, Kind: StoreErrorPreconditionFailed}
	}
	version, err := s.commitLocked(body)
	if s.ambiguousNext {
		s.ambiguousNext = false
		return "", &StoreError{Operation: StoreOperationCAS, Kind: StoreErrorOutcomeUnknown, Err: errors.New("response lost")}
	}
	if s.preconditionAfterCommitNext {
		s.preconditionAfterCommitNext = false
		return "", &StoreError{Operation: StoreOperationCAS, Kind: StoreErrorPreconditionFailed, Err: errors.New("SDK retry rejected")}
	}
	return version, err
}

func (s *memoryStore) commitLocked(body []byte) (Version, error) {
	s.version++
	version := Version(fmt.Sprintf("etag-%d", s.version))
	s.object = &StoredObject{Body: append([]byte(nil), body...), Version: version}
	return version, nil
}

func (s *memoryStore) setAmbiguousNext() {
	s.mu.Lock()
	s.ambiguousNext = true
	s.mu.Unlock()
}

func (s *memoryStore) setPreconditionAfterCommitNext() {
	s.mu.Lock()
	s.preconditionAfterCommitNext = true
	s.mu.Unlock()
}

func (s *memoryStore) remove() {
	s.mu.Lock()
	s.object = nil
	s.mu.Unlock()
}

type blockingStore struct {
	*memoryStore
	entered chan struct{}
	proceed chan struct{}
}

type orderedReadStore struct {
	mu      xsync.RBMutex
	objects []StoredObject
	entered []chan struct{}
	release chan struct{}
	reads   int
}

func (s *orderedReadStore) Get(ctx context.Context, _ Key) (StoredObject, error) {
	s.mu.Lock()
	index := s.reads
	s.reads++
	s.mu.Unlock()
	close(s.entered[index])
	if index == 0 {
		select {
		case <-s.release:
		case <-ctx.Done():
			return StoredObject{}, ctx.Err()
		}
	}
	object := s.objects[index]
	return StoredObject{Body: append([]byte(nil), object.Body...), Version: object.Version}, nil
}

func (*orderedReadStore) CreateIfAbsent(context.Context, Key, []byte) (Version, error) {
	return "", errors.New("unexpected CreateIfAbsent")
}

func (*orderedReadStore) CompareAndSwap(context.Context, Key, Version, []byte) (Version, error) {
	return "", errors.New("unexpected CompareAndSwap")
}

type pausedClock struct {
	mu  xsync.RBMutex
	now time.Time
}

func newPausedClock(now time.Time) *pausedClock {
	return &pausedClock{now: now}
}

func (c *pausedClock) Now() time.Time {
	token := c.mu.RLock()
	defer c.mu.RUnlock(token)
	return c.now
}

func (*pausedClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

func (c *pausedClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type nilClock struct{}

func (*nilClock) Now() time.Time { panic("typed-nil clock used") }

func (*nilClock) AfterFunc(time.Duration, func()) func() bool {
	panic("typed-nil clock used")
}

func (s *blockingStore) CompareAndSwap(ctx context.Context, key Key, expected Version, body []byte) (Version, error) {
	close(s.entered)
	select {
	case <-s.proceed:
		return s.memoryStore.CompareAndSwap(ctx, key, expected, body)
	case <-ctx.Done():
		return "", &StoreError{Operation: StoreOperationCAS, Kind: StoreErrorOutcomeUnknown, Err: ctx.Err()}
	}
}
