package lease

import "time"

// LeaseRecord has coordination-lifetime persisted identity through Metadata.UID
// and mutation-scoped contents through Spec. Encoding, validation, and
// transition rules belong to the lease core.
type LeaseRecord struct {
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
