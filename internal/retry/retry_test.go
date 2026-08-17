package retry_test

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/negativexq/go-retry-circuit-breaker/internal/fixture"
	"github.com/negativexq/go-retry-circuit-breaker/internal/retry"
)

func testPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts: 4,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    200 * time.Millisecond,
		Jitter:      0.2,
	}
}

func doGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func TestRetryEventuallySucceeds(t *testing.T) {
	srv := fixture.NewFlakyServer()
	defer srv.Close()

	url := srv.URL + "/fail-first?n=3&key=eventual"

	resp, result, err := retry.Do(context.Background(), testPolicy(), nil, func(ctx context.Context, attempt int) (*http.Response, error) {
		return doGet(ctx, url)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if result.Attempts != 4 {
		t.Fatalf("expected 4 attempts, got %d", result.Attempts)
	}
	if !result.Retried {
		t.Fatal("expected Retried=true")
	}
}

func TestDoesNotRetryOn400(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	_, result, err := retry.Do(context.Background(), testPolicy(), nil, func(ctx context.Context, attempt int) (*http.Response, error) {
		return doGet(ctx, srv.URL)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", result.Attempts)
	}
	if calls != 1 {
		t.Fatalf("expected upstream called once, got %d", calls)
	}
}

func TestRetryStopsOnContextCancellation(t *testing.T) {
	srv := fixture.NewFlakyServer()
	defer srv.Close()

	policy := retry.Policy{
		MaxAttempts: 10,
		BaseDelay:   1 * time.Second,
		MaxDelay:    10 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err := retry.Do(ctx, policy, nil, func(ctx context.Context, attempt int) (*http.Response, error) {
		return doGet(ctx, srv.URL+"/always-fail")
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected to stop near the deadline, took %v", elapsed)
	}
}

func TestJitterProducesVariedBoundedDelays(t *testing.T) {
	p := retry.Policy{
		MaxAttempts: 1,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Jitter:      0.2,
		Rand:        rand.New(rand.NewSource(42)),
	}

	seen := map[time.Duration]bool{}
	for i := 0; i < 8; i++ {
		d := p.Delay(3) // base*2^1 = 200ms, +/-20% => [160ms, 240ms]
		if d < 160*time.Millisecond || d > 240*time.Millisecond {
			t.Fatalf("delay %v out of expected jitter bounds [160ms, 240ms]", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected varied delays across calls, got only: %v", seen)
	}
}

func TestDelayCappedAtMaxDelay(t *testing.T) {
	p := retry.Policy{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  150 * time.Millisecond,
	}
	d := p.Delay(4) // uncapped would be 400ms
	if d != 150*time.Millisecond {
		t.Fatalf("expected delay capped at 150ms, got %v", d)
	}
}

func TestFirstAttemptIsImmediate(t *testing.T) {
	p := testPolicy()
	if d := p.Delay(1); d != 0 {
		t.Fatalf("expected attempt 1 to be immediate, got %v", d)
	}
}
