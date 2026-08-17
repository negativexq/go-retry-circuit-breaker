// Package retry implements a bounded, context-aware retry policy with
// exponential backoff and jitter for HTTP-style operations.
package retry

import (
	"context"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// Policy configures the retry behavior for a single logical operation.
type Policy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	// A value <= 0 is treated as 1 (no retries).
	MaxAttempts int

	// BaseDelay is the backoff delay before the second attempt. Subsequent
	// attempts double this value: attempt N (N>=2) waits
	// BaseDelay * 2^(N-2), capped at MaxDelay.
	BaseDelay time.Duration

	// MaxDelay caps the computed backoff delay before jitter is applied.
	MaxDelay time.Duration

	// Jitter is the +/- fraction applied to the computed delay, e.g. 0.2
	// spreads the delay across [delay*0.8, delay*1.2]. Zero disables jitter.
	Jitter float64

	// Rand, if set, is used as the source of randomness for jitter. This
	// exists so tests can inject a seeded source for deterministic
	// assertions; production code can leave it nil to use the default
	// (auto-seeded, concurrency-safe) global source.
	Rand *rand.Rand
}

// ClassifyFunc decides whether a given (response, error) outcome should be
// retried. It is called after every attempt.
type ClassifyFunc func(resp *http.Response, err error) bool

// DefaultRetryable is the default classification policy:
//
// Retried:
//   - network/transport errors (connection refused, timeouts, DNS, etc.)
//   - HTTP 429, 502, 503, 504
//   - any other 5xx (a deliberate, documented policy choice — see README)
//
// Not retried:
//   - HTTP 400, 401, 403, 404, 409, 422 and any other non-5xx status
func DefaultRetryable(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		return true
	}
	return false
}

// Result carries observability metadata about how an operation was executed.
type Result struct {
	Attempts int
	Retried  bool
}

// Do executes op, retrying according to p and classify until the outcome is
// no longer retryable, attempts are exhausted, or ctx is done.
//
// op is called once per attempt with the attempt number (1-based) so callers
// can log or annotate individual attempts.
//
// Sleeps between attempts are context-aware: if ctx is canceled while
// waiting for the next attempt, Do returns immediately with ctx.Err(). This
// means retries are bounded by both MaxAttempts and the caller's deadline.
//
// Response body ownership: every response except the one ultimately
// returned is drained and closed internally, so callers never see (and
// never need to close) an intermediate attempt's body — only the final
// *http.Response, which the caller owns and must close.
func Do(
	ctx context.Context,
	p Policy,
	classify ClassifyFunc,
	op func(ctx context.Context, attempt int) (*http.Response, error),
) (*http.Response, Result, error) {
	if classify == nil {
		classify = DefaultRetryable
	}

	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var resp *http.Response
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			d := p.Delay(attempt)
			if ra := retryAfterDelay(resp); ra > 0 {
				d = ra
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return resp, Result{Attempts: attempt - 1, Retried: attempt > 2}, ctx.Err()
			}
		}

		resp, err = op(ctx, attempt)

		if !classify(resp, err) {
			return resp, Result{Attempts: attempt, Retried: attempt > 1}, err
		}

		if attempt < maxAttempts {
			// Retrying: this response isn't the one we return, so drain
			// and close it now rather than leaking it. Draining (instead
			// of a bare Close) gives the underlying transport a chance to
			// reuse the connection for the next attempt.
			drainAndClose(resp)
		}
	}

	return resp, Result{Attempts: maxAttempts, Retried: maxAttempts > 1}, err
}

// Delay computes the backoff delay before the given attempt (1-based),
// including jitter. Attempt 1 is always immediate (zero delay).
func (p Policy) Delay(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}

	exp := attempt - 2
	d := float64(p.BaseDelay) * math.Pow(2, float64(exp))
	if p.MaxDelay > 0 && d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}

	if p.Jitter > 0 && d > 0 {
		spread := d * p.Jitter
		d = d - spread + p.jitterFloat64()*2*spread
		if d < 0 {
			d = 0
		}
	}

	return time.Duration(d)
}

// jitterFloat64 returns a random float64 in [0, 1). If Rand is set (tests
// inject a seeded source for determinism), it is used; otherwise the
// top-level math/rand functions are used, which are auto-seeded and safe
// for concurrent use.
func (p Policy) jitterFloat64() float64 {
	if p.Rand != nil {
		return p.Rand.Float64()
	}
	return rand.Float64()
}

// drainAndClose discards and closes an intermediate (non-final) response
// body so its connection can be reused and its resources released.
func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// retryAfterDelay extracts an integer-seconds Retry-After delay from resp,
// if present and valid. Non-integer (HTTP-date) formats are not supported
// in v1 and are ignored.
func retryAfterDelay(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
