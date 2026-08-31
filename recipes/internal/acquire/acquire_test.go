package acquire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
)

func TestRunDoesNotStarveObservationWithFasterRetries(t *testing.T) {
	client := &observationClient{observed: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := Run(ctx, Config{
			Client:          client,
			RetryPeriod:     5 * time.Millisecond,
			ObserveInterval: 40 * time.Millisecond,
			OnObservation: func(lease.Observation) {
				cancel()
			},
		})
		result <- err
	}()

	select {
	case <-client.observed:
	case <-time.After(time.Second):
		t.Fatal("explicit observation was starved by acquisition retries")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context cancellation", err)
	}
}

type observationClient struct {
	observed chan struct{}
}

func (*observationClient) Require(context.Context) (*lease.Lease, error) {
	return nil, lease.ErrNotEligible
}

func (*observationClient) Renew(context.Context, *lease.Lease) error {
	return errors.New("unexpected Renew")
}

func (*observationClient) Release(context.Context, *lease.Lease) error {
	return errors.New("unexpected Release")
}

func (c *observationClient) Observe(context.Context) (lease.Observation, error) {
	select {
	case c.observed <- struct{}{}:
	default:
	}
	return lease.Observation{ClientID: "leader", EpochID: 7}, nil
}

func (*observationClient) Timing() lease.Timing {
	return lease.Timing{LeaseDuration: time.Second, RenewDeadline: 500 * time.Millisecond, RequestTimeout: 50 * time.Millisecond}
}
