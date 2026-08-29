package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/cenkalti/backoff/v7"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/s3store"
)

const (
	defaultEndpoint = "http://127.0.0.1:8333"
	defaultRegion   = "us-east-1"
	defaultBucket   = "lease-tests"
	accessKey       = "lease-dev"
	secretKey       = "local-test-only"
)

type Harness struct {
	Endpoint       string
	Region         string
	Bucket         string
	RunID          string
	CandidateImage string
	ContainerTool  string
	S3             *s3.Client
}

func New(ctx context.Context) (*Harness, error) {
	endpoint := envOr("S3_LEASE_E2E_ENDPOINT", defaultEndpoint)
	region := envOr("S3_LEASE_E2E_REGION", defaultRegion)
	client, err := NewS3Client(ctx, endpoint, region)
	if err != nil {
		return nil, err
	}
	return &Harness{
		Endpoint:       endpoint,
		Region:         region,
		Bucket:         envOr("S3_LEASE_E2E_BUCKET", defaultBucket),
		RunID:          uniqueID(),
		CandidateImage: os.Getenv("S3_LEASE_E2E_CANDIDATE_IMAGE"),
		ContainerTool:  envOr("CONTAINER_TOOL", "docker"),
		S3:             client,
	}, nil
}

func NewS3Client(ctx context.Context, endpoint, region string) (*s3.Client, error) {
	config, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awsconfig.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	if err != nil {
		return nil, fmt.Errorf("load local AWS config: %w", err)
	}
	return s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	}), nil
}

func (h *Harness) Ready(ctx context.Context) error {
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		if err := h.ensureBucket(ctx); err != nil {
			return struct{}{}, err
		}
		key := h.Key("readiness")
		body := []byte("s3-lease-ready")
		if _, err := h.S3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(h.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		}); err != nil {
			return struct{}{}, err
		}
		output, err := h.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.Bucket), Key: aws.String(key)})
		if err != nil {
			return struct{}{}, err
		}
		defer output.Body.Close()
		got, err := io.ReadAll(output.Body)
		if err != nil {
			return struct{}{}, err
		}
		if !bytes.Equal(got, body) {
			return struct{}{}, errors.New("readiness round trip changed body")
		}
		return struct{}{}, nil
	}, backoff.WithBackOff(backoff.NewConstantBackOff(500*time.Millisecond)), backoff.WithMaxTries(120), backoff.WithMaxElapsedTime(time.Minute))
	if err != nil {
		return fmt.Errorf("SeaweedFS S3 readiness: %w", err)
	}
	return nil
}

func (h *Harness) ensureBucket(ctx context.Context) error {
	_, err := h.S3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(h.Bucket)})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
	}
	return err
}

func (h *Harness) Store() (lease.LeaseStore, error) {
	return s3store.New(s3store.Config{Client: h.S3})
}

func (h *Harness) Key(caseID string) string {
	return fmt.Sprintf("e2e/%s/%s/%s.json", h.RunID, caseID, uniqueID())
}

func (h *Harness) RunCandidate(ctx context.Context, args ...string) ([]byte, error) {
	if h.CandidateImage == "" {
		return nil, errors.New("S3_LEASE_E2E_CANDIDATE_IMAGE is required; run through make e2e")
	}
	endpoint, err := containerEndpoint(h.Endpoint)
	if err != nil {
		return nil, err
	}
	commandArgs := []string{
		"run", "--rm", "--add-host", "host.docker.internal:host-gateway",
		h.CandidateImage,
		"--endpoint", endpoint,
		"--region", h.Region,
		"--bucket", h.Bucket,
	}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, h.ContainerTool, commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("candidate failed: %w: %s", err, output)
	}
	return output, nil
}

func containerEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse E2E endpoint: %w", err)
	}
	host := parsed.Hostname()
	if host == "127.0.0.1" || host == "localhost" {
		port := parsed.Port()
		parsed.Host = "host.docker.internal"
		if port != "" {
			parsed.Host += ":" + port
		}
	}
	return parsed.String(), nil
}

func uniqueID() string {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strings.ReplaceAll(fmt.Sprintf("%d", time.Now().UnixNano()), "-", "")
	}
	return hex.EncodeToString(raw[:])
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
