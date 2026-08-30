package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	json "github.com/goccy/go-json"
	"github.com/puzpuzpuz/xsync/v4"
)

// Event is one structured JSON log emitted by a candidate.
type Event struct {
	Timestamp        float64 `json:"ts"`
	Message          string  `json:"msg"`
	ClientID         string  `json:"client_id"`
	ObservedClientID string  `json:"observed_client_id"`
	EpochID          uint64  `json:"epoch_id"`
	SequenceID       uint64  `json:"sequence_id"`
}

// Candidate is one independently running, deterministically named E2E
// candidate container. Its output may be inspected while it is running.
type Candidate struct {
	Name   string
	tool   string
	output *lockedBuffer
	done   chan struct{}
	err    error
}

type lockedBuffer struct {
	mu     xsync.RBMutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(value)
}

func (b *lockedBuffer) Bytes() []byte {
	token := b.mu.RLock()
	defer b.mu.RUnlock(token)
	return append([]byte(nil), b.buffer.Bytes()...)
}

// StartCandidate starts the candidate image without waiting for it to exit.
func (h *Harness) StartCandidate(ctx context.Context, args ...string) (*Candidate, error) {
	if h.CandidateImage == "" {
		return nil, errors.New("S3_LEASE_E2E_CANDIDATE_IMAGE is required; run through make e2e")
	}
	endpoint, err := containerEndpoint(h.Endpoint)
	if err != nil {
		return nil, err
	}
	name := h.nextCandidateName()
	commandArgs := []string{
		"run", "--rm", "--name", name,
		"--add-host", "host.docker.internal:host-gateway",
		h.CandidateImage,
		"--endpoint", endpoint,
		"--region", h.Region,
		"--bucket", h.Bucket,
	}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, h.ContainerTool, commandArgs...)
	output := &lockedBuffer{}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start candidate %s: %w", name, err)
	}
	candidate := &Candidate{Name: name, tool: h.ContainerTool, output: output, done: make(chan struct{})}
	go func() {
		candidate.err = command.Wait()
		close(candidate.done)
	}()
	return candidate, nil
}

func (h *Harness) nextCandidateName() string {
	h.candidateMu.Lock()
	h.candidateSequence++
	sequence := h.candidateSequence
	h.candidateMu.Unlock()
	return fmt.Sprintf("s3-lease-%s-%03d", h.RunID, sequence)
}

// Output returns a race-safe snapshot of all candidate logs received so far.
func (c *Candidate) Output() []byte {
	if c == nil || c.output == nil {
		return nil
	}
	return c.output.Bytes()
}

// Events decodes complete structured log lines received so far.
func (c *Candidate) Events() []Event {
	lines := bytes.Split(c.Output(), []byte{'\n'})
	events := make([]Event, 0, len(lines))
	for _, line := range lines {
		var event Event
		if json.Unmarshal(line, &event) == nil && event.Message != "" {
			events = append(events, event)
		}
	}
	return events
}

// WaitForEvent is a bounded event barrier over the live candidate stream.
func (c *Candidate) WaitForEvent(ctx context.Context, predicate func(Event) bool) (Event, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	seen := 0
	for {
		events := c.Events()
		for _, event := range events[seen:] {
			if predicate(event) {
				return event, nil
			}
		}
		seen = len(events)
		select {
		case <-ctx.Done():
			return Event{}, fmt.Errorf("wait for candidate %s event: %w; logs: %s", c.Name, ctx.Err(), c.Output())
		case <-c.done:
			for _, event := range c.Events()[seen:] {
				if predicate(event) {
					return event, nil
				}
			}
			return Event{}, fmt.Errorf("candidate %s exited before event; logs: %s", c.Name, c.Output())
		case <-ticker.C:
		}
	}
}

// Wait returns all logs after the candidate exits. It is safe to call more
// than once, which simplifies failure cleanup and evidence collection.
func (c *Candidate) Wait() ([]byte, error) {
	<-c.done
	output := c.Output()
	if c.err != nil {
		return output, fmt.Errorf("candidate failed: %w: %s", c.err, output)
	}
	return output, nil
}

func (c *Candidate) Pause(ctx context.Context) error {
	return c.control(ctx, "pause")
}

func (c *Candidate) Unpause(ctx context.Context) error {
	return c.control(ctx, "unpause")
}

func (c *Candidate) Kill(ctx context.Context) error {
	return c.control(ctx, "kill")
}

// Cleanup forcibly removes this one named container if it still exists.
func (c *Candidate) Cleanup(ctx context.Context) error {
	command := exec.CommandContext(ctx, c.tool, "rm", "-f", c.Name)
	output, err := command.CombinedOutput()
	if err != nil && !strings.Contains(string(output), "No such container") {
		return fmt.Errorf("cleanup candidate %s: %w: %s", c.Name, err, output)
	}
	return nil
}

func (c *Candidate) control(ctx context.Context, operation string) error {
	command := exec.CommandContext(ctx, c.tool, operation, c.Name)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s candidate %s: %w: %s", operation, c.Name, err, output)
	}
	return nil
}
