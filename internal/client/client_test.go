package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/negativexq/go-retry-circuit-breaker/internal/breaker"
	"github.com/negativexq/go-retry-circuit-breaker/internal/client"
	"github.com/negativexq/go-retry-circuit-breaker/internal/fixture"
	"github.com/negativexq/go-retry-circuit-breaker/internal/retry"
)

func fastPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts: 4,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
		Jitter:      0.1,
	}
}

func TestClientRetriesCountAsSingleBreakerSuccess(t *testing.T) {
	srv := fixture.NewFlakyServer()
	defer srv.Close()

	cb := breaker.New(breaker.Config{FailureThreshold: 2, OpenTimeout: time.Second})
	c := client.New(fastPolicy(), cb)

	resp, result, err := c.Get(context.Background(), srv.URL+"/fail-first?n=3&key=client-retry")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if result.Attempts != 4 {
		t.Fatalf("expected 4 internal attempts, got %d", result.Attempts)
	}
	if cb.State() != breaker.Closed {
		t.Fatalf("expected breaker to remain CLOSED after eventual success, got %s", cb.State())
	}
}

func TestClientOpensBreakerAfterRepeatedLogicalFailures(t *testing.T) {
	srv := fixture.NewFlakyServer()
	defer srv.Close()

	cb := breaker.New(breaker.Config{FailureThreshold: 2, OpenTimeout: time.Second})
	c := client.New(retry.Policy{
		MaxAttempts: 2,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    20 * time.Millisecond,
	}, cb)

	url := srv.URL + "/always-fail"

	// /always-fail returns 503 with a nil error, matching stdlib
	// http.Client semantics (non-2xx status is not itself a Go error) — the
	// breaker still treats it as a logical failure because the classifier
	// says a 503 is retryable, and retries were exhausted.
	for i := 1; i <= 2; i++ {
		resp, _, err := c.Get(context.Background(), url)
		if err != nil {
			t.Fatalf("operation %d: unexpected error: %v", i, err)
		}
		if resp.StatusCode != 503 {
			t.Fatalf("operation %d: expected 503, got %d", i, resp.StatusCode)
		}
		resp.Body.Close()
	}

	if got := cb.State(); got != breaker.Open {
		t.Fatalf("expected breaker OPEN after 2 failed operations, got %s", got)
	}

	_, _, err := c.Get(context.Background(), url)
	if !errors.Is(err, client.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}
