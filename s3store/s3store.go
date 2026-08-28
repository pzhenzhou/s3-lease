// Package s3store defines the AWS SDK for Go v2 adapter boundary for leases.
package s3store

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store is the process-lifetime AWS SDK-compatible service required by this
// adapter. It is the only interface through which the adapter depends on AWS.
type Store interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Config is construction-scoped and immutable for one adapter instance.
type Config struct {
	Client  Store
	Metrics RequestMetrics
	Logger  *slog.Logger
}

// adapter has process lifetime and is safe to share between Lease instances
// once implemented. It will satisfy lease.LeaseStore without exposing AWS SDK
// types to the lease core.
type adapter struct {
	config Config
}
