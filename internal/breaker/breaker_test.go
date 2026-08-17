package breaker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/negativexq/go-retry-circuit-breaker/internal/breaker"
)

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	cb := breaker.New(breaker.Config{FailureThreshold: 3, OpenTimeout: time.Second})

	upstreamCalls := 0
	call := func() error {
		if !cb.Allow() {
			return breaker.ErrCircuitOpen
		}
		upstreamCalls++
		cb.RecordFailure()
		return errors.New("upstream failure")
	}

	for i := 1; i <= 3; i++ {
		if err := call(); err == nil {
			t.Fatalf("expected failure on operation %d", i)
		}
	}

	if got := cb.State(); got != breaker.Open {
		t.Fatalf("expected breaker OPEN after %d consecutive failures, got %s", 3, got)
	}

	err := call()
	if !errors.Is(err, breaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if upstreamCalls != 3 {
		t.Fatalf("expected upstream called exactly 3 times (4th call short-circuited), got %d", upstreamCalls)
	}
}

func TestHalfOpenRecoversToClosedOnProbeSuccess(t *testing.T) {
	now := time.Now()
	cb := breaker.NewWithClock(breaker.Config{
		FailureThreshold: 1,
		OpenTimeout:      3 * time.Second,
	}, func() time.Time { return now })

	if !cb.Allow() {
		t.Fatal("expected first call to be allowed in CLOSED state")
	}
	cb.RecordFailure()
	if got := cb.State(); got != breaker.Open {
		t.Fatalf("expected OPEN after reaching threshold, got %s", got)
	}

	now = now.Add(3 * time.Second)

	if got := cb.State(); got != breaker.HalfOpen {
		t.Fatalf("expected HALF-OPEN once cooldown elapses, got %s", got)
	}
	if !cb.Allow() {
		t.Fatal("expected probe call to be allowed in HALF-OPEN state")
	}
	cb.RecordSuccess()
	if got := cb.State(); got != breaker.Closed {
		t.Fatalf("expected CLOSED after successful probe, got %s", got)
	}
}

func TestHalfOpenReopensOnProbeFailure(t *testing.T) {
	now := time.Now()
	cb := breaker.NewWithClock(breaker.Config{
		FailureThreshold: 1,
		OpenTimeout:      3 * time.Second,
	}, func() time.Time { return now })

	cb.Allow()
	cb.RecordFailure() // -> OPEN

	now = now.Add(3 * time.Second)
	if !cb.Allow() {
		t.Fatal("expected probe call to be allowed in HALF-OPEN state")
	}
	cb.RecordFailure()

	if got := cb.State(); got != breaker.Open {
		t.Fatalf("expected OPEN again after failed probe, got %s", got)
	}
}

func TestHalfOpenLimitsConcurrentProbes(t *testing.T) {
	now := time.Now()
	cb := breaker.NewWithClock(breaker.Config{
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
		HalfOpenMaxCalls: 1,
	}, func() time.Time { return now })

	cb.Allow()
	cb.RecordFailure() // -> OPEN

	now = now.Add(time.Second)

	if !cb.Allow() {
		t.Fatal("expected first probe to be allowed")
	}
	if cb.Allow() {
		t.Fatal("expected second concurrent probe to be rejected")
	}
}
