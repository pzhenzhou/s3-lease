// Package faultproxy provides the per-candidate HTTP fault boundary used by
// the E2E suite. It records every request and injects failures without changing
// production lease or S3 adapter behavior.
package faultproxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

type Outcome string

const (
	OutcomeForward          Outcome = "forward"
	OutcomeFailBefore       Outcome = "fail_before_forward"
	OutcomeSuppressResponse Outcome = "forward_and_suppress_response"
	OutcomePartitioned      Outcome = "partitioned"
)

// Rule matches the next Count requests. Empty method and path values match
// every request. A zero Count means one request.
type Rule struct {
	Method        string
	PathContains  string
	IfMatch       bool
	IfNoneMatch   bool
	BodyContains  string
	Count         int
	Delay         time.Duration
	ResponseDelay time.Duration
	Outcome       Outcome
	Status        int
}

// Trace is one immutable request record captured after handling completes.
type Trace struct {
	Method      string
	Path        string
	StartedAt   time.Time
	Duration    time.Duration
	IfMatch     string
	IfNoneMatch string
	Forwarded   bool
	Outcome     Outcome
	Status      int
}

// Proxy is one candidate-scoped reverse proxy.
type Proxy struct {
	mu        xsync.RBMutex
	upstream  *url.URL
	endpoint  string
	client    *http.Client
	server    *http.Server
	listener  net.Listener
	rules     []Rule
	traces    []Trace
	partition bool
}

// New starts a proxy bound to an ephemeral host port.
func New(upstream string) (*Proxy, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse fault-proxy upstream: %w", err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen for fault proxy: %w", err)
	}
	proxy := &Proxy{
		upstream: target,
		listener: listener,
		client:   &http.Client{Transport: http.DefaultTransport},
	}
	proxy.endpoint = "http://127.0.0.1:" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	proxy.server = &http.Server{Handler: proxy, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

// Endpoint returns the host-visible proxy URL.
func (p *Proxy) Endpoint() string { return p.endpoint }

// AddRule appends a request fault rule.
func (p *Proxy) AddRule(rule Rule) {
	if rule.Count <= 0 {
		rule.Count = 1
	}
	if rule.Outcome == "" {
		rule.Outcome = OutcomeForward
	}
	p.mu.Lock()
	p.rules = append(p.rules, rule)
	p.mu.Unlock()
}

// SetPartition toggles failure-before-forwarding for every request.
func (p *Proxy) SetPartition(partition bool) {
	p.mu.Lock()
	p.partition = partition
	p.mu.Unlock()
}

// Traces returns a stable copy of all completed request traces.
func (p *Proxy) Traces() []Trace {
	token := p.mu.RLock()
	defer p.mu.RUnlock(token)
	return append([]Trace(nil), p.traces...)
}

func (p *Proxy) Close(ctx context.Context) error { return p.server.Shutdown(ctx) }

func (p *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	trace := Trace{
		Method:      request.Method,
		Path:        request.URL.RequestURI(),
		StartedAt:   started,
		IfMatch:     request.Header.Get("If-Match"),
		IfNoneMatch: request.Header.Get("If-None-Match"),
		Outcome:     OutcomeForward,
	}
	defer func() {
		trace.Duration = time.Since(started)
		p.mu.Lock()
		p.traces = append(p.traces, trace)
		p.mu.Unlock()
	}()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		trace.Status = http.StatusBadRequest
		http.Error(writer, err.Error(), trace.Status)
		return
	}
	rule, partitioned := p.takeRule(request, body)

	if partitioned {
		trace.Outcome = OutcomePartitioned
		trace.Status = http.StatusServiceUnavailable
		http.Error(writer, "candidate partitioned by E2E proxy", trace.Status)
		return
	}
	if rule.Delay > 0 {
		timer := time.NewTimer(rule.Delay)
		select {
		case <-request.Context().Done():
			timer.Stop()
			trace.Outcome = OutcomeFailBefore
			trace.Status = http.StatusGatewayTimeout
			return
		case <-timer.C:
		}
	}
	if rule.Outcome == OutcomeFailBefore {
		trace.Outcome = OutcomeFailBefore
		trace.Status = rule.Status
		if trace.Status == 0 {
			trace.Status = http.StatusServiceUnavailable
		}
		http.Error(writer, "injected failure before forwarding", trace.Status)
		return
	}

	target := *p.upstream
	target.Path = singleJoiningSlash(p.upstream.Path, request.URL.Path)
	target.RawQuery = request.URL.RawQuery
	forward, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		trace.Status = http.StatusBadGateway
		http.Error(writer, err.Error(), trace.Status)
		return
	}
	forward.Header = request.Header.Clone()
	// SigV4 signs the original Host value. The transport still connects to the
	// upstream URL, but preserving Host keeps the forwarded request verifiable.
	forward.Host = request.Host
	response, err := p.client.Do(forward)
	trace.Forwarded = true
	if err != nil {
		trace.Status = http.StatusBadGateway
		http.Error(writer, err.Error(), trace.Status)
		return
	}
	defer response.Body.Close()
	trace.Status = response.StatusCode
	if rule.ResponseDelay > 0 {
		timer := time.NewTimer(rule.ResponseDelay)
		<-timer.C
	}
	if rule.Outcome == OutcomeSuppressResponse {
		trace.Outcome = OutcomeSuppressResponse
		_, _ = io.Copy(io.Discard, response.Body)
		if hijacker, ok := writer.(http.Hijacker); ok {
			connection, _, hijackErr := hijacker.Hijack()
			if hijackErr == nil {
				_ = connection.Close()
				return
			}
		}
		return
	}
	for name, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func (p *Proxy) takeRule(request *http.Request, body []byte) (Rule, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.partition {
		return Rule{}, true
	}
	for index := range p.rules {
		rule := &p.rules[index]
		if rule.Method != "" && rule.Method != request.Method ||
			rule.PathContains != "" && !strings.Contains(request.URL.Path, rule.PathContains) ||
			rule.IfMatch && request.Header.Get("If-Match") == "" ||
			rule.IfNoneMatch && request.Header.Get("If-None-Match") == "" ||
			rule.BodyContains != "" && !bytes.Contains(body, []byte(rule.BodyContains)) {
			continue
		}
		selected := *rule
		rule.Count--
		if rule.Count == 0 {
			p.rules = append(p.rules[:index], p.rules[index+1:]...)
		}
		return selected, false
	}
	return Rule{}, false
}

func singleJoiningSlash(left, right string) string {
	return strings.TrimRight(left, "/") + "/" + strings.TrimLeft(right, "/")
}
