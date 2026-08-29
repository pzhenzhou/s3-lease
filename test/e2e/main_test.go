//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pzhenzhou/s3-lease/test/e2e/internal/harness"
)

var testHarness *harness.Harness

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var err error
	testHarness, err = harness.New(ctx)
	if err == nil {
		err = testHarness.Ready(ctx)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "E2E setup failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
