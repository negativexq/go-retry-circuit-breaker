// Package client wires retry.Policy and breaker.CircuitBreaker together
// into a single HTTP client that behaves predictably against a flaky
// upstream.
//
// Ordering: the circuit breaker guards the *logical operation*, not each
// individual HTTP call. A single Get performs up to Policy.MaxAttempts
// retries internally; only the aggregate outcome is reported to the
// breaker. All attempts failing records exactly one breaker failure, and an
// eventual success after retries records exactly one breaker success.
package client

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/negativexq/go-retry-circuit-breaker/internal/breaker"
	"github.com/negativexq/go-retry-circuit-breaker/internal/retry"
)

// ErrCircuitOpen is returned when the breaker rejects a call without
// contacting the upstream at all.
var ErrCircuitOpen = breaker.ErrCircuitOpen

// Result carries observability metadata about a completed Get call.
type Result struct {
	Attempts     int
	Retried      bool
	BreakerState string
}

// Client performs GET requests against an unreliable upstream, applying a
// retry.Policy behind a breaker.CircuitBreaker.
type Client struct {
	httpClient *http.Client
	policy     retry.Policy
	breaker    *breaker.CircuitBreaker
	classify   retry.ClassifyFunc
	logger     *slog.Logger
}

// Option customizes a Client at construction time.
type Option func(*Client)

// WithHTTPClient overrides the underlying *http.Client (default: http.DefaultClient).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithClassifier overrides which outcomes are considered retryable
// (default: retry.DefaultRetryable).
func WithClassifier(fn retry.ClassifyFunc) Option {
	return func(c *Client) { c.classify = fn }
}

// WithLogger overrides the *slog.Logger used for per-attempt logging
// (default: slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

// New creates a Client with the given retry policy and circuit breaker.
func New(policy retry.Policy, cb *breaker.CircuitBreaker, opts ...Option) *Client {
	c := &Client{
		httpClient: http.DefaultClient,
		policy:     policy,
		breaker:    cb,
		classify:   retry.DefaultRetryable,
		logger:     slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get performs a GET request as a single logical operation: the breaker is
// consulted once up front, the retry policy runs internally against the
// upstream, and the aggregate outcome is reported back to the breaker
// exactly once.
func (c *Client) Get(ctx context.Context, url string) (*http.Response, Result, error) {
	if !c.breaker.Allow() {
		return nil, Result{BreakerState: c.breaker.State().String()}, ErrCircuitOpen
	}

	resp, res, err := retry.Do(ctx, c.policy, c.classify, func(ctx context.Context, attempt int) (*http.Response, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if reqErr != nil {
			return nil, reqErr
		}
		resp, doErr := c.httpClient.Do(req)
		c.logger.Debug("retry attempt",
			"attempt", attempt,
			"url", url,
			"status", statusOf(resp),
			"err", doErr,
		)
		return resp, doErr
	})

	c.recordOutcome(resp, err)

	return resp, Result{
		Attempts:     res.Attempts,
		Retried:      res.Retried,
		BreakerState: c.breaker.State().String(),
	}, err
}

// recordOutcome reports the operation's aggregate outcome to the breaker.
// A non-retryable error (e.g. a caller-side mistake like a malformed
// request) is not treated as an upstream health signal and leaves the
// breaker untouched; only genuine success or an exhausted-retries failure
// move the breaker.
func (c *Client) recordOutcome(resp *http.Response, err error) {
	retryable := c.classify(resp, err)
	switch {
	case err == nil && !retryable:
		c.breaker.RecordSuccess()
	case retryable:
		c.breaker.RecordFailure()
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}
