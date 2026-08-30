// Package fencedresource implements a test-only CAS-protected manifest whose
// durable epoch watermark rejects stale leaders.
package fencedresource

import (
	"context"
	"errors"
	"fmt"
	"time"

	json "github.com/goccy/go-json"
	"github.com/pzhenzhou/s3-lease/lease"
)

var ErrFenced = errors.New("protected resource rejected stale epoch")

const maxCASAttempts = 32

// Mutation is one durably accepted protected-resource update.
type Mutation struct {
	EpochID   uint64    `json:"epochID"`
	Value     string    `json:"value"`
	AppliedAt time.Time `json:"appliedAt"`
}

// State is the complete durable resource state.
type State struct {
	EpochWatermark uint64     `json:"epochWatermark"`
	History        []Mutation `json:"history"`
}

// Resource binds fencing state to one independently CAS-protected S3 object.
type Resource struct {
	store lease.LeaseStore
	key   lease.Key
}

func New(store lease.LeaseStore, key lease.Key) *Resource {
	return &Resource{store: store, key: key}
}

// Apply atomically validates epochID, advances the watermark, and appends the
// mutation. Conditional-write conflicts are reread rather than hidden.
func (r *Resource) Apply(ctx context.Context, epochID uint64, value string) (State, error) {
	for range maxCASAttempts {
		object, err := r.store.Get(ctx, r.key)
		if errors.Is(err, lease.ErrNotFound) {
			state := nextState(State{}, epochID, value)
			body, encodeErr := json.Marshal(state)
			if encodeErr != nil {
				return State{}, encodeErr
			}
			if _, createErr := r.store.CreateIfAbsent(ctx, r.key, body); createErr == nil {
				return state, nil
			} else if errors.Is(createErr, lease.ErrConflict) {
				continue
			} else {
				return State{}, createErr
			}
		}
		if err != nil {
			return State{}, err
		}
		state, err := decode(object.Body)
		if err != nil {
			return State{}, err
		}
		if epochID < state.EpochWatermark {
			return state, fmt.Errorf("%w: proposed epoch %d is below watermark %d", ErrFenced, epochID, state.EpochWatermark)
		}
		next := nextState(state, epochID, value)
		body, err := json.Marshal(next)
		if err != nil {
			return State{}, err
		}
		if _, err = r.store.CompareAndSwap(ctx, r.key, object.Version, body); err == nil {
			return next, nil
		} else if !errors.Is(err, lease.ErrConflict) {
			return State{}, err
		}
	}
	return State{}, fmt.Errorf("protected resource exceeded %d CAS attempts: %w", maxCASAttempts, lease.ErrConflict)
}

func (r *Resource) Read(ctx context.Context) (State, error) {
	object, err := r.store.Get(ctx, r.key)
	if err != nil {
		return State{}, err
	}
	return decode(object.Body)
}

func nextState(current State, epochID uint64, value string) State {
	history := append([]Mutation(nil), current.History...)
	history = append(history, Mutation{EpochID: epochID, Value: value, AppliedAt: time.Now().UTC()})
	return State{EpochWatermark: max(current.EpochWatermark, epochID), History: history}
}

func decode(body []byte) (State, error) {
	var state State
	if err := json.Unmarshal(body, &state); err != nil {
		return State{}, fmt.Errorf("decode protected resource: %w", err)
	}
	return state, nil
}
