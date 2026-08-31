// Package fencedmanifest demonstrates an S3-resident fencing boundary for
// application state protected by a lease epoch.
package fencedmanifest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	json "github.com/goccy/go-json"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/s3-lease/lease"
)

const (
	apiVersion           = "coordination.pactdata.io/v1alpha1"
	kind                 = "FencedManifest"
	maxManifestBodyBytes = 64 << 10
	maxPayloadBytes      = 40 << 10
	maxMutationIDBytes   = 256
	maxMutationAttempts  = 8
	maxHistoryEntries    = 8
)

var (
	ErrFenced              = errors.New("manifest mutation fenced")
	ErrEpochNotActivated   = errors.New("manifest epoch is not activated")
	ErrMutationConflict    = errors.New("manifest mutation ID conflict")
	ErrConcurrentMutation  = errors.New("manifest mutation already in progress")
	ErrResourceDisappeared = errors.New("fenced manifest disappeared")
	ErrResourceChanged     = errors.New("fenced manifest identity changed")
	ErrResourceRolledBack  = errors.New("fenced manifest rolled back")
	ErrInvalidManifest     = errors.New("invalid fenced manifest")
)

type operation uint8

const (
	operationActivate operation = iota + 1
	operationPublish
)

// Manifest is an immutable snapshot of one protected resource. Payload is a
// caller-owned copy of the opaque JSON committed by the latest publication.
type Manifest struct {
	ResourceUID     string
	AcceptedEpochID uint64
	Revision        uint64
	LastMutationID  string
	Payload         []byte
	History         []HistoryEntry
}

// HistoryEntry identifies one recently committed activation or publication.
// History is bounded and intended for reconciliation and safety diagnostics.
type HistoryEntry struct {
	EpochID    uint64
	MutationID string
	Activation bool
}

type manifestRecord struct {
	APIVersion      string          `json:"apiVersion"`
	Kind            string          `json:"kind"`
	ResourceUID     string          `json:"resourceUID"`
	AcceptedEpochID uint64          `json:"acceptedEpochID"`
	Revision        uint64          `json:"revision"`
	Payload         json.RawMessage `json:"payload"`
	History         []historyRecord `json:"history"`
}

type historyRecord struct {
	EpochID        uint64 `json:"epochID"`
	MutationID     string `json:"mutationID"`
	MutationDigest string `json:"mutationDigest"`
	Activation     bool   `json:"activation,omitempty"`
}

type mutationRequest struct {
	operation  operation
	epochID    uint64
	mutationID string
	payload    []byte
	digest     string
}

type proposal struct {
	request   mutationRequest
	expected  lease.Version
	body      []byte
	result    Manifest
	create    bool
	ambiguous bool
}

// Writer is process-scoped and bound to one protected manifest. It permits
// one unresolved logical mutation so a later call can reconcile exact bytes.
type Writer struct {
	store lease.LeaseStore
	key   lease.Key

	mu           xsync.RBMutex
	resourceUID  string
	lastEpoch    uint64
	lastRevision uint64
	lastDigest   [sha256.Size]byte
	pending      *proposal
}

// NewWriter validates the storage binding for one protected manifest.
func NewWriter(store lease.LeaseStore, key lease.Key) (*Writer, error) {
	if isNil(store) {
		return nil, fmt.Errorf("%w: lease store is required", lease.ErrInvalidConfig)
	}
	if strings.TrimSpace(key.Bucket) == "" || key.ObjectKey == "" {
		return nil, fmt.Errorf("%w: manifest bucket and object key are required", lease.ErrInvalidConfig)
	}
	return &Writer{store: store, key: key}, nil
}

// Activate advances the resource watermark before a new leader becomes ready.
// Repeating an already accepted epoch is an idempotent read without a write.
func (w *Writer) Activate(ctx context.Context, epochID uint64, mutationID string) (Manifest, error) {
	request, err := newRequest(operationActivate, epochID, mutationID, nil)
	if err != nil {
		return Manifest{}, err
	}
	return w.mutate(ctx, request)
}

// Publish atomically verifies the activated epoch and replaces the opaque JSON
// payload. The same mutation ID must always describe the same proposal.
func (w *Writer) Publish(ctx context.Context, epochID uint64, mutationID string, payload []byte) (Manifest, error) {
	request, err := newRequest(operationPublish, epochID, mutationID, payload)
	if err != nil {
		return Manifest{}, err
	}
	return w.mutate(ctx, request)
}

// Read returns one informational manifest snapshot. It does not grant lease
// authority and cannot activate an epoch.
func (w *Writer) Read(ctx context.Context) (Manifest, error) {
	if w == nil {
		return Manifest{}, fmt.Errorf("%w: nil manifest writer", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return Manifest{}, fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if !w.mu.TryLock() {
		return Manifest{}, ErrConcurrentMutation
	}
	defer w.mu.Unlock()
	record, _, err := w.read(ctx)
	if errors.Is(err, lease.ErrNotFound) && w.resourceUID != "" {
		return Manifest{}, ErrResourceDisappeared
	}
	if err != nil {
		return Manifest{}, err
	}
	return snapshot(record), nil
}

func (w *Writer) mutate(ctx context.Context, request mutationRequest) (Manifest, error) {
	if w == nil {
		return Manifest{}, fmt.Errorf("%w: nil manifest writer", lease.ErrInvalidConfig)
	}
	if ctx == nil {
		return Manifest{}, fmt.Errorf("%w: context is required", lease.ErrInvalidConfig)
	}
	if !w.mu.TryLock() {
		return Manifest{}, ErrConcurrentMutation
	}
	defer w.mu.Unlock()

	if w.pending != nil && !sameRequest(w.pending.request, request) {
		return Manifest{}, fmt.Errorf("%w: a different manifest proposal remains unresolved", lease.ErrUnknownOutcome)
	}

	var lastErr error
	for range maxMutationAttempts {
		if err := ctx.Err(); err != nil {
			if w.pending != nil && w.pending.ambiguous {
				return Manifest{}, errors.Join(lease.ErrUnknownOutcome, err)
			}
			return Manifest{}, err
		}
		if w.pending != nil {
			result, resolved, rebase, err := w.resolvePending(ctx)
			if resolved || err != nil {
				if err != nil && w.pending != nil && w.pending.ambiguous {
					return result, errors.Join(lease.ErrUnknownOutcome, err)
				}
				return result, err
			}
			if !rebase {
				lastErr = lease.ErrUnknownOutcome
				continue
			}
		}

		record, version, err := w.read(ctx)
		if errors.Is(err, lease.ErrNotFound) {
			if w.resourceUID != "" {
				return Manifest{}, ErrResourceDisappeared
			}
			if request.operation != operationActivate {
				return Manifest{}, ErrEpochNotActivated
			}
			record, err = initialRecord(request)
			if err != nil {
				return Manifest{}, err
			}
			version = ""
		} else if err != nil {
			return Manifest{}, err
		} else {
			result, done, prepareErr := prepareExisting(record, request)
			if done || prepareErr != nil {
				return result, prepareErr
			}
			record = resultRecord(record, request)
		}

		body, err := encode(record)
		if err != nil {
			return Manifest{}, err
		}
		w.pending = &proposal{
			request:  request,
			expected: version,
			body:     body,
			result:   snapshot(record),
			create:   version == "",
		}
		result, err := w.sendPending(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !ambiguousOrConflict(err) {
			w.pending = nil
			return Manifest{}, err
		}
	}
	if w.pending != nil {
		if w.pending.ambiguous {
			return Manifest{}, fmt.Errorf("%w: manifest proposal unresolved after %d attempts: %v",
				lease.ErrUnknownOutcome, maxMutationAttempts, lastErr)
		}
		w.pending = nil
	}
	return Manifest{}, fmt.Errorf("%w: manifest mutation exceeded %d attempts: %v",
		lease.ErrConflict, maxMutationAttempts, lastErr)
}

func (w *Writer) resolvePending(ctx context.Context) (Manifest, bool, bool, error) {
	pending := w.pending
	record, version, err := w.read(ctx)
	if errors.Is(err, lease.ErrNotFound) {
		if !pending.create {
			if pending.ambiguous {
				return Manifest{}, false, false, errors.Join(lease.ErrUnknownOutcome, ErrResourceDisappeared)
			}
			w.pending = nil
			return Manifest{}, false, false, ErrResourceDisappeared
		}
		result, sendErr := w.sendPending(ctx)
		return result, sendErr == nil, false, sendErr
	}
	if err != nil {
		if !pending.ambiguous {
			w.pending = nil
		}
		return Manifest{}, false, false, err
	}
	matched, conflict := historyStatus(record.History, pending.request)
	if matched {
		w.pending = nil
		return snapshot(record), true, false, nil
	}
	if conflict {
		if pending.ambiguous {
			return Manifest{}, false, false, errors.Join(lease.ErrUnknownOutcome, ErrMutationConflict)
		}
		w.pending = nil
		return Manifest{}, false, false, ErrMutationConflict
	}
	if record.AcceptedEpochID > pending.request.epochID {
		fenced := fencedError(pending.request.epochID, record.AcceptedEpochID)
		if pending.ambiguous {
			return Manifest{}, false, false, errors.Join(lease.ErrUnknownOutcome, fenced)
		}
		w.pending = nil
		return Manifest{}, false, false, fenced
	}
	if version == pending.expected {
		result, sendErr := w.sendPending(ctx)
		return result, sendErr == nil, false, sendErr
	}
	if pending.ambiguous {
		return Manifest{}, false, false, fmt.Errorf("%w: pending manifest mutation is absent from bounded history", lease.ErrUnknownOutcome)
	}

	w.pending = nil
	return Manifest{}, false, true, nil
}

func historyStatus(history []historyRecord, request mutationRequest) (matched, conflict bool) {
	for _, entry := range history {
		if entry.MutationID != request.mutationID {
			continue
		}
		if entry.MutationDigest == request.digest {
			return true, false
		}
		conflict = true
	}
	return false, conflict
}

func (w *Writer) sendPending(ctx context.Context) (Manifest, error) {
	pending := w.pending
	var err error
	if pending.create {
		_, err = w.store.CreateIfAbsent(ctx, w.key, pending.body)
	} else {
		_, err = w.store.CompareAndSwap(ctx, w.key, pending.expected, pending.body)
	}
	if err != nil {
		pending.ambiguous = pending.ambiguous || unknownWrite(err)
		return Manifest{}, err
	}
	w.remember(pending.result, pending.body)
	w.pending = nil
	return cloneManifest(pending.result), nil
}

func (w *Writer) read(ctx context.Context) (manifestRecord, lease.Version, error) {
	object, err := w.store.Get(ctx, w.key)
	if err != nil {
		return manifestRecord{}, "", err
	}
	record, err := decode(object.Body)
	if err != nil {
		return manifestRecord{}, "", err
	}
	if w.resourceUID != "" && w.resourceUID != record.ResourceUID {
		return manifestRecord{}, "", ErrResourceChanged
	}
	digest := sha256.Sum256(object.Body)
	if record.AcceptedEpochID < w.lastEpoch || record.Revision < w.lastRevision ||
		record.Revision == w.lastRevision && w.lastRevision != 0 && digest != w.lastDigest {
		return manifestRecord{}, "", ErrResourceRolledBack
	}
	w.resourceUID = record.ResourceUID
	w.lastEpoch = record.AcceptedEpochID
	w.lastRevision = record.Revision
	w.lastDigest = digest
	return record, object.Version, nil
}

func (w *Writer) remember(manifest Manifest, body []byte) {
	w.resourceUID = manifest.ResourceUID
	w.lastEpoch = manifest.AcceptedEpochID
	w.lastRevision = manifest.Revision
	w.lastDigest = sha256.Sum256(body)
}

func newRequest(op operation, epochID uint64, mutationID string, payload []byte) (mutationRequest, error) {
	if epochID == 0 {
		return mutationRequest{}, fmt.Errorf("%w: epoch must be positive", lease.ErrInvalidConfig)
	}
	mutationID = strings.TrimSpace(mutationID)
	if mutationID == "" || len(mutationID) > maxMutationIDBytes {
		return mutationRequest{}, fmt.Errorf("%w: mutation ID must contain 1..%d bytes", lease.ErrInvalidConfig, maxMutationIDBytes)
	}
	if op == operationPublish {
		if len(payload) == 0 || len(payload) > maxPayloadBytes || !json.Valid(payload) {
			return mutationRequest{}, fmt.Errorf("%w: payload must be valid JSON containing 1..%d bytes", lease.ErrInvalidConfig, maxPayloadBytes)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, payload); err != nil {
			return mutationRequest{}, fmt.Errorf("%w: compact payload: %v", lease.ErrInvalidConfig, err)
		}
		payload = compact.Bytes()
	}
	request := mutationRequest{operation: op, epochID: epochID, mutationID: mutationID, payload: append([]byte(nil), payload...)}
	request.digest = requestDigest(request)
	return request, nil
}

func initialRecord(request mutationRequest) (manifestRecord, error) {
	uid, err := newUID()
	if err != nil {
		return manifestRecord{}, fmt.Errorf("create manifest UID: %w", err)
	}
	return manifestRecord{
		APIVersion:      apiVersion,
		Kind:            kind,
		ResourceUID:     uid,
		AcceptedEpochID: request.epochID,
		Revision:        1,
		Payload:         json.RawMessage("null"),
		History:         []historyRecord{newHistoryRecord(request)},
	}, nil
}

func prepareExisting(record manifestRecord, request mutationRequest) (Manifest, bool, error) {
	current := snapshot(record)
	if request.epochID < record.AcceptedEpochID {
		return current, false, fencedError(request.epochID, record.AcceptedEpochID)
	}
	matched, conflict := historyStatus(record.History, request)
	if matched {
		return current, true, nil
	}
	if conflict {
		return current, false, ErrMutationConflict
	}
	if request.operation == operationActivate && request.epochID == record.AcceptedEpochID {
		return current, true, nil
	}
	if request.operation == operationPublish && request.epochID > record.AcceptedEpochID {
		return current, false, ErrEpochNotActivated
	}
	if record.Revision == math.MaxUint64 {
		return current, false, fmt.Errorf("%w: revision overflow", ErrInvalidManifest)
	}
	return current, false, nil
}

func resultRecord(current manifestRecord, request mutationRequest) manifestRecord {
	current.AcceptedEpochID = max(current.AcceptedEpochID, request.epochID)
	current.Revision++
	current.History = append(append([]historyRecord(nil), current.History...), newHistoryRecord(request))
	if len(current.History) > maxHistoryEntries {
		current.History = append([]historyRecord(nil), current.History[len(current.History)-maxHistoryEntries:]...)
	}
	if request.operation == operationPublish {
		current.Payload = append(json.RawMessage(nil), request.payload...)
	}
	return current
}

func encode(record manifestRecord) ([]byte, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode fenced manifest: %w", err)
	}
	if len(body) > maxManifestBodyBytes {
		return nil, fmt.Errorf("%w: encoded manifest exceeds %d bytes", ErrInvalidManifest, maxManifestBodyBytes)
	}
	return body, nil
}

func decode(body []byte) (manifestRecord, error) {
	if len(body) > maxManifestBodyBytes {
		return manifestRecord{}, ErrInvalidManifest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var record manifestRecord
	if err := decoder.Decode(&record); err != nil {
		return manifestRecord{}, fmt.Errorf("%w: decode: %v", ErrInvalidManifest, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return manifestRecord{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidManifest)
	}
	if record.APIVersion != apiVersion || record.Kind != kind || record.ResourceUID == "" ||
		record.AcceptedEpochID == 0 || record.Revision == 0 || !validHex(record.ResourceUID, 16) ||
		len(record.Payload) == 0 || len(record.Payload) > maxPayloadBytes || !json.Valid(record.Payload) ||
		!validHistory(record) {
		return manifestRecord{}, ErrInvalidManifest
	}
	return record, nil
}

func snapshot(record manifestRecord) Manifest {
	history := make([]HistoryEntry, len(record.History))
	for index, entry := range record.History {
		history[index] = HistoryEntry{EpochID: entry.EpochID, MutationID: entry.MutationID, Activation: entry.Activation}
	}
	lastMutationID := ""
	if len(history) != 0 {
		lastMutationID = history[len(history)-1].MutationID
	}
	return Manifest{
		ResourceUID:     record.ResourceUID,
		AcceptedEpochID: record.AcceptedEpochID,
		Revision:        record.Revision,
		LastMutationID:  lastMutationID,
		Payload:         append([]byte(nil), record.Payload...),
		History:         history,
	}
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.Payload = append([]byte(nil), manifest.Payload...)
	manifest.History = append([]HistoryEntry(nil), manifest.History...)
	return manifest
}

func newHistoryRecord(request mutationRequest) historyRecord {
	return historyRecord{
		EpochID:        request.epochID,
		MutationID:     request.mutationID,
		MutationDigest: request.digest,
		Activation:     request.operation == operationActivate,
	}
}

func validHistory(record manifestRecord) bool {
	expectedLength := min(record.Revision, uint64(maxHistoryEntries))
	if uint64(len(record.History)) != expectedLength {
		return false
	}
	completeHistory := record.Revision <= uint64(maxHistoryEntries)
	seen := make(map[string]struct{}, len(record.History))
	var previousEpoch uint64
	for index, entry := range record.History {
		if entry.EpochID == 0 || entry.EpochID > record.AcceptedEpochID || entry.EpochID < previousEpoch ||
			entry.MutationID == "" || len(entry.MutationID) > maxMutationIDBytes ||
			!validHex(entry.MutationDigest, sha256.Size) {
			return false
		}
		if _, exists := seen[entry.MutationID]; exists {
			return false
		}
		seen[entry.MutationID] = struct{}{}
		if index == 0 && completeHistory && !entry.Activation {
			return false
		}
		if index > 0 && entry.Activation != (entry.EpochID > previousEpoch) {
			return false
		}
		previousEpoch = entry.EpochID
	}
	return previousEpoch == record.AcceptedEpochID
}

func requestDigest(request mutationRequest) string {
	hash := sha256.New()
	hash.Write([]byte{byte(request.operation)})
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], request.epochID)
	hash.Write(epoch[:])
	hash.Write([]byte(request.mutationID))
	hash.Write([]byte{0})
	hash.Write(request.payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func newUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func sameRequest(left, right mutationRequest) bool {
	return left.operation == right.operation && left.epochID == right.epochID &&
		left.mutationID == right.mutationID && left.digest == right.digest
}

func ambiguousOrConflict(err error) bool {
	return unknownWrite(err) || errors.Is(err, lease.ErrConflict)
}

func unknownWrite(err error) bool {
	return errors.Is(err, lease.ErrUnknownOutcome) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func fencedError(proposed, accepted uint64) error {
	return fmt.Errorf("%w: proposed epoch %d is below accepted epoch %d", ErrFenced, proposed, accepted)
}

func validHex(value string, bytes int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
