// Package storecontract defines backend qualification shared by local S3
// fixtures and the real-AWS release gate.
package storecontract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
)

// Config binds the qualification suite to one store and a fresh-key factory.
type Config struct {
	Store      lease.LeaseStore
	NewKey     func(string) lease.Key
	Rounds     int
	Contenders int
	Timeout    time.Duration
}

type writeResult struct {
	index   int
	version lease.Version
	err     error
}

// Run verifies strong reads and conditional-write behavior. Every round uses
// fresh keys so the suite never deletes or restores a coordination object.
func Run(t *testing.T, config Config) {
	t.Helper()
	if config.Rounds <= 0 {
		config.Rounds = 100
	}
	if config.Rounds < 100 {
		t.Fatalf("store qualification requires at least 100 rounds, got %d", config.Rounds)
	}
	if config.Contenders <= 0 {
		config.Contenders = 32
	}
	if config.Contenders < 32 {
		t.Fatalf("store qualification requires at least 32 contenders, got %d", config.Contenders)
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Minute
	}

	t.Run("conditional writes and strong reads", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
		defer cancel()
		for round := range config.Rounds {
			key := config.NewKey(fmt.Sprintf("conditional-%03d", round))
			body1 := []byte(fmt.Sprintf(`{"round":%d,"value":"first"}`, round))
			if _, err := config.Store.Get(ctx, key); !errors.Is(err, lease.ErrNotFound) {
				t.Fatalf("round %d missing GET = %v, want ErrNotFound", round, err)
			}
			version1, err := config.Store.CreateIfAbsent(ctx, key, body1)
			if err != nil {
				t.Fatalf("round %d create: %v", round, err)
			}
			assertObject(t, ctx, config.Store, key, body1, version1)
			if _, err := config.Store.CreateIfAbsent(ctx, key, []byte("duplicate")); !errors.Is(err, lease.ErrConflict) {
				t.Fatalf("round %d duplicate create = %v, want ErrConflict", round, err)
			}

			body2 := []byte(fmt.Sprintf(`{"round":%d,"value":"second"}`, round))
			version2, err := config.Store.CompareAndSwap(ctx, key, version1, body2)
			if err != nil {
				t.Fatalf("round %d CAS: %v", round, err)
			}
			if version2 == "" || version2 == version1 {
				t.Fatalf("round %d CAS version = %q, want change from %q", round, version2, version1)
			}
			if _, err := config.Store.CompareAndSwap(ctx, key, version1, []byte("stale")); !errors.Is(err, lease.ErrConflict) {
				t.Fatalf("round %d stale CAS = %v, want ErrConflict", round, err)
			}
			assertObject(t, ctx, config.Store, key, body2, version2)
		}
	})

	t.Run("concurrent creation has one winner", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
		defer cancel()
		for round := range config.Rounds {
			key := config.NewKey(fmt.Sprintf("create-race-%03d", round))
			body := func(index int) []byte { return []byte(fmt.Sprintf("candidate-%d", index)) }
			results := raceWrites(config.Contenders, func(index int) (lease.Version, error) {
				return config.Store.CreateIfAbsent(ctx, key, body(index))
			})
			assertRaceWinner(t, ctx, config.Store, key, round, config.Contenders, body, results)
		}
	})

	t.Run("concurrent compare-and-swap has one winner", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
		defer cancel()
		for round := range config.Rounds {
			key := config.NewKey(fmt.Sprintf("cas-race-%03d", round))
			expected, err := config.Store.CreateIfAbsent(ctx, key, []byte(fmt.Sprintf("seed-%d", round)))
			if err != nil {
				t.Fatalf("round %d seed create: %v", round, err)
			}
			body := func(index int) []byte { return []byte(fmt.Sprintf("candidate-%d", index)) }
			results := raceWrites(config.Contenders, func(index int) (lease.Version, error) {
				return config.Store.CompareAndSwap(ctx, key, expected, body(index))
			})
			assertRaceWinner(t, ctx, config.Store, key, round, config.Contenders, body, results)
		}
	})
}

func raceWrites(contenders int, write func(int) (lease.Version, error)) []writeResult {
	start := make(chan struct{})
	completed := make(chan writeResult, contenders)
	for index := range contenders {
		go func() {
			<-start
			version, err := write(index)
			completed <- writeResult{index: index, version: version, err: err}
		}()
	}
	close(start)
	results := make([]writeResult, 0, contenders)
	for range contenders {
		results = append(results, <-completed)
	}
	return results
}

func assertRaceWinner(t *testing.T, ctx context.Context, store lease.LeaseStore, key lease.Key, round, contenders int,
	body func(int) []byte, results []writeResult,
) {
	t.Helper()
	var winners []writeResult
	conflicts := 0
	errs := make([]error, 0, len(results))
	for _, result := range results {
		errs = append(errs, result.err)
		if result.err == nil {
			winners = append(winners, result)
		} else if errors.Is(result.err, lease.ErrConflict) {
			conflicts++
		}
	}
	if len(winners) != 1 || conflicts != contenders-1 {
		t.Fatalf("round %d: wins=%d conflicts=%d errors=%v", round, len(winners), conflicts, errs)
	}
	winner := winners[0]
	assertObject(t, ctx, store, key, body(winner.index), winner.version)
}

func assertObject(t *testing.T, ctx context.Context, store lease.LeaseStore, key lease.Key, body []byte, version lease.Version) {
	t.Helper()
	if version == "" {
		t.Fatal("conditional write returned an empty ETag")
	}
	object, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(object.Body, body) || object.Version != version {
		t.Fatalf("GET = (%q, %q), want (%q, %q)", object.Body, object.Version, body, version)
	}
}
