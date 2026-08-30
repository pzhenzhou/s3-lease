package lease

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	json "github.com/goccy/go-json"
)

// Record has coordination-lifetime persisted identity through Metadata.UID
// and mutation-scoped contents through Spec. Encoding, validation, and
// transition rules belong to the lease core.
type Record struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   RecordMetadata `json:"metadata"`
	Spec       RecordSpec     `json:"spec"`
}

// RecordMetadata is stable for the lifetime of one lease object.
type RecordMetadata struct {
	Name      string    `json:"name,omitempty"`
	UID       string    `json:"uid"`
	CreatedAt time.Time `json:"createdAt"`
}

// RecordSpec is replaced atomically on every committed lease mutation.
type RecordSpec struct {
	ClientID             string    `json:"clientID"`
	LeaseDurationSeconds uint64    `json:"leaseDurationSeconds"`
	AcquireTime          time.Time `json:"acquireTime"`
	RenewTime            time.Time `json:"renewTime"`
	EpochID              uint64    `json:"epochID"`
	SequenceID           uint64    `json:"sequenceID"`
}

func encodeRecord(record Record) ([]byte, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode lease record: %w", err)
	}
	return body, nil
}

func decodeRecord(body []byte) (Record, error) {
	var record Record
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return Record{}, fmt.Errorf("%w: decode lease record: %v", ErrProtocolViolation, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Record{}, fmt.Errorf("%w: trailing lease record data: %v", ErrProtocolViolation, err)
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func validateRecord(record Record) error {
	switch {
	case record.APIVersion != APIVersion:
		return fmt.Errorf("%w: unsupported apiVersion %q", ErrProtocolViolation, record.APIVersion)
	case record.Kind != Kind:
		return fmt.Errorf("%w: unsupported kind %q", ErrProtocolViolation, record.Kind)
	case record.Metadata.UID == "":
		return fmt.Errorf("%w: empty lease UID", ErrProtocolViolation)
	case record.Metadata.CreatedAt.IsZero():
		return fmt.Errorf("%w: zero creation time", ErrProtocolViolation)
	case record.Spec.LeaseDurationSeconds == 0:
		return fmt.Errorf("%w: zero lease duration", ErrProtocolViolation)
	case record.Spec.LeaseDurationSeconds > uint64(math.MaxInt64/int64(time.Second)):
		return fmt.Errorf("%w: lease duration overflows local time", ErrProtocolViolation)
	case record.Spec.AcquireTime.IsZero(), record.Spec.RenewTime.IsZero():
		return fmt.Errorf("%w: zero lease timestamp", ErrProtocolViolation)
	case record.Spec.EpochID == 0, record.Spec.SequenceID == 0:
		return fmt.Errorf("%w: zero lease counter", ErrProtocolViolation)
	}
	return nil
}

func recordsCompatible(previous, next Record) error {
	if previous.Metadata.UID != next.Metadata.UID {
		return fmt.Errorf("%w: lease UID changed", ErrProtocolViolation)
	}
	if next.Spec.EpochID < previous.Spec.EpochID ||
		(next.Spec.EpochID == previous.Spec.EpochID && next.Spec.SequenceID < previous.Spec.SequenceID) {
		return fmt.Errorf("%w: lease counters rolled back", ErrProtocolViolation)
	}
	if next.Spec.EpochID == previous.Spec.EpochID && next.Spec.SequenceID == previous.Spec.SequenceID &&
		!sameRecord(previous, next) {
		return fmt.Errorf("%w: incompatible records share epoch/sequence", ErrProtocolViolation)
	}
	return nil
}

func sameRecord(a, b Record) bool {
	return a.APIVersion == b.APIVersion &&
		a.Kind == b.Kind &&
		a.Metadata.Name == b.Metadata.Name &&
		a.Metadata.UID == b.Metadata.UID &&
		a.Metadata.CreatedAt.Equal(b.Metadata.CreatedAt) &&
		a.Spec.ClientID == b.Spec.ClientID &&
		a.Spec.LeaseDurationSeconds == b.Spec.LeaseDurationSeconds &&
		a.Spec.AcquireTime.Equal(b.Spec.AcquireTime) &&
		a.Spec.RenewTime.Equal(b.Spec.RenewTime) &&
		a.Spec.EpochID == b.Spec.EpochID &&
		a.Spec.SequenceID == b.Spec.SequenceID
}

func newInitialRecord(config Config, now time.Time) (Record, error) {
	uid, err := newUUIDv4()
	if err != nil {
		return Record{}, err
	}
	wall := now.UTC()
	return Record{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata: RecordMetadata{
			Name:      config.MetadataName,
			UID:       uid,
			CreatedAt: wall,
		},
		Spec: RecordSpec{
			ClientID:             config.ClientID,
			LeaseDurationSeconds: uint64(config.LeaseDuration / time.Second),
			AcquireTime:          wall,
			RenewTime:            wall,
			EpochID:              1,
			SequenceID:           1,
		},
	}, nil
}

func acquisitionRecord(previous Record, config Config, now time.Time) (Record, error) {
	if previous.Spec.EpochID == math.MaxUint64 {
		return Record{}, ErrCounterOverflow
	}
	next := previous
	next.Spec.ClientID = config.ClientID
	next.Spec.LeaseDurationSeconds = uint64(config.LeaseDuration / time.Second)
	next.Spec.AcquireTime = now.UTC()
	next.Spec.RenewTime = now.UTC()
	next.Spec.EpochID++
	next.Spec.SequenceID = 1
	return next, nil
}

func renewalRecord(previous Record, now time.Time) (Record, error) {
	if previous.Spec.SequenceID == math.MaxUint64 {
		return Record{}, ErrCounterOverflow
	}
	next := previous
	next.Spec.RenewTime = now.UTC()
	next.Spec.SequenceID++
	return next, nil
}

func releaseRecord(previous Record, now time.Time) (Record, error) {
	next, err := renewalRecord(previous, now)
	if err != nil {
		return Record{}, err
	}
	next.Spec.ClientID = ""
	return next, nil
}

func newUUIDv4() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate lease UID: %w", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])
	return string(encoded), nil
}
