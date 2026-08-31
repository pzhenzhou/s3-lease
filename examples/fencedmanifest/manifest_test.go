package fencedmanifest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
)

func TestWriterActivationPublicationAndFencing(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	ctx := context.Background()

	activated, err := writer.Activate(ctx, 1, "activate-1")
	if err != nil {
		t.Fatal(err)
	}
	if activated.AcceptedEpochID != 1 || activated.Revision != 1 || string(activated.Payload) != "null" {
		t.Fatalf("initial activation = %+v payload=%s", activated, activated.Payload)
	}
	idempotent, err := writer.Activate(ctx, 1, "another-activation-name")
	if err != nil {
		t.Fatal(err)
	}
	if idempotent.Revision != activated.Revision {
		t.Fatalf("idempotent activation revision = %d, want %d", idempotent.Revision, activated.Revision)
	}

	published, err := writer.Publish(ctx, 1, "publish-1", []byte(`{ "value": 1 }`))
	if err != nil {
		t.Fatal(err)
	}
	if published.Revision != 2 || string(published.Payload) != `{"value":1}` {
		t.Fatalf("publication = %+v payload=%s", published, published.Payload)
	}
	if _, err := writer.Publish(ctx, 2, "publish-before-activation", []byte(`{"value":2}`)); !errors.Is(err, ErrEpochNotActivated) {
		t.Fatalf("future publication = %v, want ErrEpochNotActivated", err)
	}

	second, err := writer.Activate(ctx, 2, "activate-2")
	if err != nil {
		t.Fatal(err)
	}
	if second.AcceptedEpochID != 2 || second.Revision != 3 || string(second.Payload) != `{"value":1}` {
		t.Fatalf("second activation = %+v payload=%s", second, second.Payload)
	}
	if _, err := writer.Publish(ctx, 1, "stale", []byte(`{"value":0}`)); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale publication = %v, want ErrFenced", err)
	}

	if _, err := writer.Publish(ctx, 2, "publish-2", []byte(`{"value":2}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Publish(ctx, 2, "publish-2", []byte(`{"value":3}`)); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("mutation ID reuse = %v, want ErrMutationConflict", err)
	}
}

func TestWriterReconcilesUnknownOutcomeWithExactProposal(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	ctx := context.Background()
	if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
		t.Fatal(err)
	}
	store.ambiguousCommit = true

	published, err := writer.Publish(ctx, 1, "publish", []byte(`{"ready":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if published.LastMutationID != "publish" || string(published.Payload) != `{"ready":true}` {
		t.Fatalf("reconciled publication = %+v payload=%s", published, published.Payload)
	}
	if calls := store.compareCalls(); calls != 1 {
		t.Fatalf("CAS calls = %d, want one committed request followed by readback", calls)
	}
}

func TestWriterRetriesExactProposalWhenUnknownWriteDidNotCommit(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	ctx := context.Background()
	if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
		t.Fatal(err)
	}
	store.ambiguousDrop = true

	if _, err := writer.Publish(ctx, 1, "publish", []byte(`{"ready":true}`)); err != nil {
		t.Fatal(err)
	}
	if calls := store.compareCalls(); calls != 2 {
		t.Fatalf("CAS calls = %d, want unknown attempt and exact resend", calls)
	}
	if !store.retriedExactBody() {
		t.Fatal("unknown proposal was not retried with exact bytes")
	}
}

func TestWriterFindsAmbiguousCommitBehindInterveningPublication(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	other := newTestWriter(t, store)
	ctx := context.Background()
	if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
		t.Fatal(err)
	}
	var interveningErr error
	store.mu.Lock()
	store.ambiguousCommit = true
	store.afterAmbiguousGet = func() {
		_, interveningErr = other.Publish(ctx, 1, "intervening", []byte(`{"value":2}`))
	}
	store.mu.Unlock()

	resolved, err := writer.Publish(ctx, 1, "ambiguous", []byte(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if interveningErr != nil {
		t.Fatalf("intervening publication: %v", interveningErr)
	}
	if resolved.Revision != 3 || resolved.LastMutationID != "intervening" || string(resolved.Payload) != `{"value":2}` {
		t.Fatalf("resolved manifest = %+v payload=%s", resolved, resolved.Payload)
	}
	if !containsMutation(resolved.History, "ambiguous") || !containsMutation(resolved.History, "intervening") {
		t.Fatalf("resolved history = %+v", resolved.History)
	}
}

func TestWriterKeepsOlderMutationIDsIdempotentWithinHistory(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	ctx := context.Background()
	if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Publish(ctx, 1, "first", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	latest, err := writer.Publish(ctx, 1, "second", []byte(`{"value":2}`))
	if err != nil {
		t.Fatal(err)
	}
	calls := store.compareCalls()
	replayed, err := writer.Publish(ctx, 1, "first", []byte(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Revision != latest.Revision || string(replayed.Payload) != string(latest.Payload) {
		t.Fatalf("replay changed manifest: latest=%+v replayed=%+v", latest, replayed)
	}
	if store.compareCalls() != calls {
		t.Fatal("idempotent historical replay submitted another CAS")
	}
	if _, err := writer.Publish(ctx, 1, "first", []byte(`{"value":3}`)); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("conflicting historical replay = %v, want ErrMutationConflict", err)
	}
}

func TestWriterValidatesBoundedHistory(t *testing.T) {
	ctx := context.Background()

	t.Run("incomplete history before cap", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Publish(ctx, 1, "publish", []byte(`true`)); err != nil {
			t.Fatal(err)
		}
		record := store.record(t)
		record.History = append([]historyRecord(nil), record.History[1:]...)
		store.replace(t, record)
		if _, err := writer.Read(ctx); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("Read = %v, want ErrInvalidManifest", err)
		}
	})

	t.Run("complete history starts with activation", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		record := store.record(t)
		record.History[0].Activation = false
		store.replace(t, record)
		if _, err := writer.Read(ctx); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("Read = %v, want ErrInvalidManifest", err)
		}
	})

	t.Run("truncated history at cap", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		for revision := 2; revision <= maxHistoryEntries+1; revision++ {
			mutationID := fmt.Sprintf("publish-%d", revision)
			payload := []byte(fmt.Sprintf(`{"revision":%d}`, revision))
			if _, err := writer.Publish(ctx, 1, mutationID, payload); err != nil {
				t.Fatal(err)
			}
		}
		manifest, err := writer.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Revision != maxHistoryEntries+1 || len(manifest.History) != maxHistoryEntries {
			t.Fatalf("manifest = revision %d history %d, want revision %d history %d",
				manifest.Revision, len(manifest.History), maxHistoryEntries+1, maxHistoryEntries)
		}
		if manifest.History[0].Activation {
			t.Fatal("first retained entry unexpectedly required activation after truncation")
		}
		record := store.record(t)
		record.History = append([]historyRecord(nil), record.History[1:]...)
		store.replace(t, record)
		if _, err := writer.Read(ctx); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("Read incomplete capped history = %v, want ErrInvalidManifest", err)
		}
	})
}

func TestWriterRejectsConcurrentMutation(t *testing.T) {
	store := &manifestStore{}
	writer := newTestWriter(t, store)
	if _, err := writer.Activate(context.Background(), 1, "activate"); err != nil {
		t.Fatal(err)
	}
	store.casEntered = make(chan struct{}, 1)
	store.unblockCAS = make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := writer.Publish(context.Background(), 1, "first", []byte(`1`))
		result <- err
	}()
	select {
	case <-store.casEntered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not reach CAS")
	}
	if _, err := writer.Publish(context.Background(), 1, "second", []byte(`2`)); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("concurrent Publish = %v, want ErrConcurrentMutation", err)
	}
	close(store.unblockCAS)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestWriterFailsClosedOnDisappearanceIdentityChangeAndOverflow(t *testing.T) {
	ctx := context.Background()

	t.Run("disappearance", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		store.remove()
		if _, err := writer.Read(ctx); !errors.Is(err, ErrResourceDisappeared) {
			t.Fatalf("Read = %v, want ErrResourceDisappeared", err)
		}
	})

	t.Run("identity change", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		record := store.record(t)
		record.ResourceUID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		store.replace(t, record)
		if _, err := writer.Read(ctx); !errors.Is(err, ErrResourceChanged) {
			t.Fatalf("Read = %v, want ErrResourceChanged", err)
		}
	})

	t.Run("revision overflow", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		record := store.record(t)
		record.Revision = math.MaxUint64
		store.replace(t, record)
		if _, err := writer.Publish(ctx, 1, "publish", []byte(`true`)); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("Publish = %v, want ErrInvalidManifest", err)
		}
	})

	t.Run("malformed record", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		store.replaceRaw([]byte(`{"unknown":true}`))
		if _, err := writer.Read(ctx); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("Read = %v, want ErrInvalidManifest", err)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		store := &manifestStore{}
		writer := newTestWriter(t, store)
		if _, err := writer.Activate(ctx, 1, "activate"); err != nil {
			t.Fatal(err)
		}
		old := store.record(t)
		if _, err := writer.Publish(ctx, 1, "publish", []byte(`true`)); err != nil {
			t.Fatal(err)
		}
		store.replace(t, old)
		if _, err := writer.Read(ctx); !errors.Is(err, ErrResourceRolledBack) {
			t.Fatalf("Read = %v, want ErrResourceRolledBack", err)
		}
	})
}

func newTestWriter(t *testing.T, store lease.LeaseStore) *Writer {
	t.Helper()
	writer, err := NewWriter(store, lease.Key{Bucket: "bucket", ObjectKey: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

type manifestStore struct {
	mu                xsync.RBMutex
	object            *lease.StoredObject
	version           uint64
	casCalls          int
	ambiguousCommit   bool
	ambiguousDrop     bool
	afterAmbiguousGet func()
	runAfterAmbiguous bool
	firstCASBody      []byte
	exactRetry        bool
	casEntered        chan struct{}
	unblockCAS        chan struct{}
}

func (s *manifestStore) Get(context.Context, lease.Key) (lease.StoredObject, error) {
	s.mu.Lock()
	var afterGet func()
	if s.runAfterAmbiguous {
		afterGet = s.afterAmbiguousGet
		s.runAfterAmbiguous = false
		s.afterAmbiguousGet = nil
	}
	s.mu.Unlock()
	if afterGet != nil {
		afterGet()
	}
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	if s.object == nil {
		return lease.StoredObject{}, storeError(lease.StoreOperationGet, lease.StoreErrorNotFound, nil)
	}
	return lease.StoredObject{Body: append([]byte(nil), s.object.Body...), Version: s.object.Version}, nil
}

func (s *manifestStore) CreateIfAbsent(_ context.Context, _ lease.Key, body []byte) (lease.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.object != nil {
		return "", storeError(lease.StoreOperationCreateIfAbsent, lease.StoreErrorPreconditionFailed, nil)
	}
	return s.commit(body), nil
}

func (s *manifestStore) CompareAndSwap(_ context.Context, _ lease.Key, expected lease.Version, body []byte) (lease.Version, error) {
	if s.casEntered != nil {
		s.casEntered <- struct{}{}
		<-s.unblockCAS
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casCalls++
	if s.casCalls == 1 {
		s.firstCASBody = append([]byte(nil), body...)
	} else if string(s.firstCASBody) == string(body) {
		s.exactRetry = true
	}
	if s.ambiguousDrop {
		s.ambiguousDrop = false
		return "", storeError(lease.StoreOperationCAS, lease.StoreErrorOutcomeUnknown, errors.New("response lost before commit"))
	}
	if s.object == nil || s.object.Version != expected {
		return "", storeError(lease.StoreOperationCAS, lease.StoreErrorPreconditionFailed, nil)
	}
	version := s.commit(body)
	if s.ambiguousCommit {
		s.ambiguousCommit = false
		s.runAfterAmbiguous = true
		return "", storeError(lease.StoreOperationCAS, lease.StoreErrorOutcomeUnknown, errors.New("response lost after commit"))
	}
	return version, nil
}

func containsMutation(history []HistoryEntry, mutationID string) bool {
	for _, entry := range history {
		if entry.MutationID == mutationID {
			return true
		}
	}
	return false
}

func (s *manifestStore) commit(body []byte) lease.Version {
	s.version++
	version := lease.Version(fmt.Sprintf("etag-%d", s.version))
	s.object = &lease.StoredObject{Body: append([]byte(nil), body...), Version: version}
	return version
}

func (s *manifestStore) compareCalls() int {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	return s.casCalls
}

func (s *manifestStore) retriedExactBody() bool {
	token := s.mu.RLock()
	defer s.mu.RUnlock(token)
	return s.exactRetry
}

func (s *manifestStore) remove() {
	s.mu.Lock()
	s.object = nil
	s.mu.Unlock()
}

func (s *manifestStore) record(t *testing.T) manifestRecord {
	t.Helper()
	object, err := s.Get(context.Background(), lease.Key{})
	if err != nil {
		t.Fatal(err)
	}
	record, err := decode(object.Body)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (s *manifestStore) replace(t *testing.T, record manifestRecord) {
	t.Helper()
	body, err := encode(record)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.commit(body)
	s.mu.Unlock()
}

func (s *manifestStore) replaceRaw(body []byte) {
	s.mu.Lock()
	s.commit(body)
	s.mu.Unlock()
}

func storeError(operation lease.StoreOperation, kind lease.StoreErrorKind, err error) error {
	return &lease.StoreError{Operation: operation, Kind: kind, Err: err}
}
