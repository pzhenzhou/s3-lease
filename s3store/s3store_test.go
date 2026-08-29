package s3store

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pzhenzhou/s3-lease/lease"
)

func TestClassifyGetNotFoundUsesS3ErrorCode(t *testing.T) {
	tests := []struct {
		name string
		code string
		kind lease.StoreErrorKind
	}{
		{name: "missing key", code: "NoSuchKey", kind: lease.StoreErrorNotFound},
		{name: "missing bucket", code: "NoSuchBucket", kind: lease.StoreErrorInvalidResponse},
		{name: "ambiguous status only", kind: lease.StoreErrorInvalidResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyError(lease.StoreOperationGet, s3Error(http.StatusNotFound, tt.code))
			kind, ok := lease.StoreErrorKindOf(err)
			if !ok || kind != tt.kind {
				t.Fatalf("classifyError kind = %q, %v; want %q", kind, ok, tt.kind)
			}
			if tt.code == "NoSuchBucket" && errors.Is(err, lease.ErrNotFound) {
				t.Fatal("NoSuchBucket classified as ErrNotFound")
			}
		})
	}
}

func s3Error(status int, code string) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err: &smithy.GenericAPIError{
			Code:    code,
			Message: code,
			Fault:   smithy.FaultClient,
		},
	}
}
