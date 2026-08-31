//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/test/internal/storecontract"
)

func TestStoreContract(t *testing.T) {
	store, err := testHarness.Store()
	if err != nil {
		t.Fatal(err)
	}
	storecontract.Run(t, storecontract.Config{
		Store: store,
		NewKey: func(name string) lease.Key {
			return lease.Key{Bucket: testHarness.Bucket, ObjectKey: testHarness.Key(name)}
		},
		Rounds:     100,
		Contenders: 32,
		Timeout:    5 * time.Minute,
	})
}
