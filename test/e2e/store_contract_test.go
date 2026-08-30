//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/samber/lo"
)

func TestStoreConditionalWriteContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	key := lease.Key{Bucket: testHarness.Bucket, ObjectKey: testHarness.Key("store-contract")}

	if _, err := store.Get(ctx, key); !errors.Is(err, lease.ErrNotFound) {
		t.Fatalf("missing GET error = %v, want ErrNotFound", err)
	}
	body1 := []byte(`{"value":"first","opaque":1}`)
	version1, err := store.CreateIfAbsent(ctx, key, body1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if version1 == "" {
		t.Fatal("create returned empty ETag")
	}
	object, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("get created object: %v", err)
	}
	if !bytes.Equal(object.Body, body1) || object.Version != version1 {
		t.Fatalf("GET = (%q, %q), want exact body and ETag (%q, %q)", object.Body, object.Version, body1, version1)
	}
	if _, err := store.CreateIfAbsent(ctx, key, []byte("duplicate")); !errors.Is(err, lease.ErrConflict) {
		t.Fatalf("duplicate create error = %v, want ErrConflict", err)
	}

	body2 := []byte(`{"value":"second","opaque":2}`)
	version2, err := store.CompareAndSwap(ctx, key, version1, body2)
	if err != nil {
		t.Fatalf("CAS: %v", err)
	}
	if version2 == "" || version2 == version1 {
		t.Fatalf("CAS ETag = %q, want nonempty opaque change from %q", version2, version1)
	}
	if _, err := store.CompareAndSwap(ctx, key, version1, []byte("stale")); !errors.Is(err, lease.ErrConflict) {
		t.Fatalf("stale CAS error = %v, want ErrConflict", err)
	}
	object, err = store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(object.Body, body2) || object.Version != version2 {
		t.Fatalf("stale CAS changed object: body=%q version=%q", object.Body, object.Version)
	}
}

func TestStoreConcurrentCreationHasOneWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	key := lease.Key{Bucket: testHarness.Bucket, ObjectKey: testHarness.Key("store-create-race")}
	const candidates = 32
	results := make(chan error, candidates)
	for index := range candidates {
		go func() {
			_, createErr := store.CreateIfAbsent(ctx, key, []byte(fmt.Sprintf("candidate-%d", index)))
			results <- createErr
		}()
	}
	errs := make([]error, 0, candidates)
	for range candidates {
		errs = append(errs, <-results)
	}
	wins := lo.CountBy(errs, func(err error) bool { return err == nil })
	conflicts := lo.CountBy(errs, func(err error) bool { return errors.Is(err, lease.ErrConflict) })
	if wins != 1 || conflicts != candidates-1 {
		t.Fatalf("results: wins=%d conflicts=%d errors=%v", wins, conflicts, errs)
	}
}
