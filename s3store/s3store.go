// Package s3store adapts the AWS SDK for Go v2 to lease.LeaseStore.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/pkg/common"
	"go.uber.org/zap"
)

const maxRecordBodyBytes = 64 << 10

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
	Logger  *zap.Logger
}

type adapter struct {
	client Store
	logger *zap.Logger
}

// New validates config and returns a backend-neutral conditional lease store.
func New(config Config) (_ lease.LeaseStore, err error) {
	logger := config.Logger
	if logger == nil {
		logger = common.Logger()
	}
	logger.Debug("S3 lease store construction started")
	defer func() {
		if err != nil {
			logger.Error("S3 lease store construction failed", zap.Error(err))
		}
	}()
	if isNilStore(config.Client) {
		return nil, fmt.Errorf("%w: S3 client is required", lease.ErrInvalidConfig)
	}
	logger.Info("S3 lease store constructed")
	return &adapter{client: config.Client, logger: logger}, nil
}

func (a *adapter) Get(ctx context.Context, key lease.Key) (_ lease.StoredObject, err error) {
	a.logger.Debug("S3 lease GET started", keyFields(key)...)
	defer func() { a.logError(lease.StoreOperationGet, err) }()
	output, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(key.Bucket),
		Key:    aws.String(key.ObjectKey),
	})
	if err != nil {
		return lease.StoredObject{}, classifyError(lease.StoreOperationGet, err)
	}
	if output == nil || output.Body == nil || output.ETag == nil || *output.ETag == "" {
		if output != nil && output.Body != nil {
			_ = output.Body.Close()
		}
		return lease.StoredObject{}, storeError(lease.StoreOperationGet, lease.StoreErrorInvalidResponse, errors.New("successful GET omitted body or ETag"))
	}
	body, readErr := io.ReadAll(io.LimitReader(output.Body, maxRecordBodyBytes+1))
	closeErr := output.Body.Close()
	if readErr != nil {
		return lease.StoredObject{}, storeError(lease.StoreOperationGet, lease.StoreErrorTemporary, readErr)
	}
	if closeErr != nil {
		return lease.StoredObject{}, storeError(lease.StoreOperationGet, lease.StoreErrorTemporary, closeErr)
	}
	if len(body) > maxRecordBodyBytes {
		return lease.StoredObject{}, storeError(lease.StoreOperationGet, lease.StoreErrorInvalidResponse, fmt.Errorf("record exceeds %d bytes", maxRecordBodyBytes))
	}
	a.logger.Debug("S3 lease GET succeeded",
		append(keyFields(key), zap.String("version", *output.ETag), zap.Int("body_bytes", len(body)))...)
	return lease.StoredObject{Body: body, Version: lease.Version(*output.ETag)}, nil
}

func (a *adapter) CreateIfAbsent(ctx context.Context, key lease.Key, body []byte) (_ lease.Version, err error) {
	a.logger.Debug("S3 lease create-if-absent started", append(keyFields(key), zap.Int("body_bytes", len(body)))...)
	defer func() { a.logError(lease.StoreOperationCreateIfAbsent, err) }()
	return a.put(ctx, lease.StoreOperationCreateIfAbsent, key, "", body)
}

func (a *adapter) CompareAndSwap(ctx context.Context, key lease.Key, expected lease.Version, body []byte) (_ lease.Version, err error) {
	a.logger.Debug("S3 lease CAS started",
		append(keyFields(key), zap.String("expected_version", string(expected)), zap.Int("body_bytes", len(body)))...)
	defer func() { a.logError(lease.StoreOperationCAS, err) }()
	if expected == "" {
		return "", storeError(lease.StoreOperationCAS, lease.StoreErrorInvalidResponse, errors.New("empty expected ETag"))
	}
	return a.put(ctx, lease.StoreOperationCAS, key, expected, body)
}

func (a *adapter) put(ctx context.Context, operation lease.StoreOperation, key lease.Key, expected lease.Version, body []byte) (lease.Version, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(key.Bucket),
		Key:         aws.String(key.ObjectKey),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}
	if operation == lease.StoreOperationCreateIfAbsent {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(string(expected))
	}
	a.logger.Debug("S3 conditional write sending",
		zap.String("operation", string(operation)),
		zap.String("if_match", string(expected)),
		zap.Bool("if_none_match_star", operation == lease.StoreOperationCreateIfAbsent))
	output, err := a.client.PutObject(ctx, input)
	if err != nil {
		return "", classifyError(operation, err)
	}
	if output == nil || output.ETag == nil || *output.ETag == "" {
		return "", storeError(operation, lease.StoreErrorOutcomeUnknown, errors.New("successful conditional write omitted ETag"))
	}
	a.logger.Info("S3 conditional write confirmed",
		append(keyFields(key), zap.String("operation", string(operation)), zap.String("version", *output.ETag))...)
	return lease.Version(*output.ETag), nil
}

func classifyError(operation lease.StoreOperation, err error) error {
	status := httpStatus(err)
	code := apiCode(err)
	write := operation != lease.StoreOperationGet

	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden || code == "accessdenied" || code == "unauthorized":
		return storeError(operation, lease.StoreErrorUnauthorized, err)
	case operation == lease.StoreOperationGet && (code == "nosuchkey" || code == "notfound"):
		return storeError(operation, lease.StoreErrorNotFound, err)
	case write && status == http.StatusPreconditionFailed:
		return storeError(operation, lease.StoreErrorPreconditionFailed, err)
	case write && status == http.StatusConflict:
		return storeError(operation, lease.StoreErrorConflict, err)
	case write && (status >= 500 || isTransportFailure(err)):
		return storeError(operation, lease.StoreErrorOutcomeUnknown, err)
	case !write && (status == http.StatusTooManyRequests || status >= 500 || isTransportFailure(err)):
		return storeError(operation, lease.StoreErrorTemporary, err)
	default:
		return storeError(operation, lease.StoreErrorInvalidResponse, err)
	}
}

func httpStatus(err error) int {
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.HTTPStatusCode()
	}
	return 0
}

func apiCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return strings.ToLower(apiErr.ErrorCode())
	}
	return ""
}

func isTransportFailure(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) || httpStatus(err) == 0
}

func storeError(operation lease.StoreOperation, kind lease.StoreErrorKind, err error) error {
	return &lease.StoreError{Operation: operation, Kind: kind, Err: err}
}

func (a *adapter) logError(operation lease.StoreOperation, err error) {
	if err != nil {
		a.logger.Error("S3 lease store method failed", zap.String("operation", string(operation)), zap.Error(err))
	}
}

func keyFields(key lease.Key) []zap.Field {
	return []zap.Field{zap.String("bucket", key.Bucket), zap.String("object_key", key.ObjectKey)}
}

func isNilStore(store Store) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
