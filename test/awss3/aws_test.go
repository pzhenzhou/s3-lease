//go:build awss3

package awss3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/lease/s3store"
	"github.com/pzhenzhou/s3-lease/test/internal/storecontract"
	"go.uber.org/zap"
)

func TestAWSStoreContractAndBucketPolicy(t *testing.T) {
	bucket := requiredEnv(t, "S3_LEASE_AWS_BUCKET")
	region := requiredEnv(t, "AWS_REGION")
	prefix := strings.Trim(requiredEnv(t, "S3_LEASE_AWS_PREFIX"), "/")
	runID := randomID(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	config, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(config)
	store, err := s3store.New(s3store.Config{Client: client, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	newKey := func(name string) lease.Key {
		return lease.Key{Bucket: bucket, ObjectKey: fmt.Sprintf("%s/%s/%s.json", prefix, runID, name)}
	}

	storecontract.Run(t, storecontract.Config{
		Store: store, NewKey: newKey, Rounds: 100, Contenders: 32, Timeout: 15 * time.Minute,
	})

	t.Run("bucket prerequisites", func(t *testing.T) {
		versioning, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		if versioning.Status != types.BucketVersioningStatusEnabled {
			t.Fatalf("bucket Versioning = %q, want Enabled", versioning.Status)
		}
		encryption, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		if encryption.ServerSideEncryptionConfiguration == nil ||
			len(encryption.ServerSideEncryptionConfiguration.Rules) == 0 {
			t.Fatal("bucket has no default server-side encryption rule")
		}
	})

	t.Run("unconditional writes and deletion are denied", func(t *testing.T) {
		key := newKey("policy")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(key.Bucket), Key: aws.String(key.ObjectKey), Body: bytes.NewReader([]byte(`{"unsafe":true}`)),
		})
		if !accessDenied(err) {
			t.Fatalf("unconditional PutObject = %v, want AccessDenied", err)
		}
		if _, err := store.CreateIfAbsent(ctx, key, []byte(`{"safe":true}`)); err != nil {
			t.Fatal(err)
		}
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(key.Bucket), Key: aws.String(key.ObjectKey)})
		if !accessDenied(err) {
			t.Fatalf("DeleteObject = %v, want AccessDenied", err)
		}
	})
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for the AWS qualification gate", name)
	}
	return value
}

func randomID(t *testing.T) string {
	t.Helper()
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value[:])
}

func accessDenied(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && strings.EqualFold(apiErr.ErrorCode(), "AccessDenied")
}
