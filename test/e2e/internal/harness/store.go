package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectSnapshot captures authoritative S3 evidence before test cleanup.
type ObjectSnapshot struct {
	Body []byte
	ETag string
}

func (h *Harness) Snapshot(ctx context.Context, key string) (ObjectSnapshot, error) {
	output, err := h.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.Bucket), Key: aws.String(key)})
	if err != nil {
		return ObjectSnapshot{}, err
	}
	defer output.Body.Close()
	body, err := io.ReadAll(output.Body)
	if err != nil {
		return ObjectSnapshot{}, err
	}
	return ObjectSnapshot{Body: body, ETag: aws.ToString(output.ETag)}, nil
}

// RestartStore restarts SeaweedFS without removing its Compose volume, then
// waits for the existing fixture to become readable again.
func (h *Harness) RestartStore(ctx context.Context) error {
	command := exec.CommandContext(ctx, h.ContainerTool, "compose", "-p", h.ComposeProject,
		"-f", h.ComposeFile, "restart", "seaweedfs")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restart SeaweedFS: %w: %s", err, output)
	}
	if err := h.Ready(ctx); err != nil {
		return err
	}
	return nil
}

// PutRaw writes test evidence outside the lease protocol. It is intended only
// for malformed-state and recovery qualification.
func (h *Harness) PutRaw(ctx context.Context, key string, body []byte) error {
	_, err := h.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.Bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	return err
}
