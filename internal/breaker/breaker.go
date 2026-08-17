// Package breaker implements a concurrency-safe circuit breaker with the
// classic CLOSED -> OPEN -> HALF-OPEN -> CLOSED state machine.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State is one of Closed, Open, or HalfOpen.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned by callers (see internal/client) when the
// breaker rejects a call in the OPEN state.
var ErrCircuitOpen = errors.New("breaker: circuit open")

// Config configures the breaker's failure sensitivity and recovery timing.
type Config struct {
	// FailureThreshold is the number of consecutive failures in the CLOSED
	// state required to trip the breaker OPEN.
	FailureThreshold int

	// OpenTimeout is how long the breaker stays OPEN before allowing a
	// HALF-OPEN probe.
	OpenTimeout time.Duration

	// HalfOpenMaxCalls is reserved for future use. v1 hard-caps HALF-OPEN
	// to a single in-flight probe regardless of this value: allowing
	// multiple concurrent probes raises an ordering question (a probe that
	// succeeds and closes the circuit, followed by a still-in-flight probe
	// that then fails, would incorrectly count as a CLOSED-state failure)
	// that needs more than a config knob to resolve correctly.
	HalfOpenMaxCalls int
}

// CircuitBreaker tracks upstream health via Allow/RecordSuccess/RecordFailure.
// This shape (rather than a single Execute wrapper) keeps the state machine
// visible at call sites and lets callers decide what counts as a failure.
type CircuitBreaker struct {
	cfg Config
	now func() time.Time

	mu               sync.Mutex
	state            State
	consecutiveFails int
	openedAt         time.Time
	halfOpenInFlight int
}

// New creates a CircuitBreaker using the real wall clock.
func New(cfg Config) *CircuitBreaker {
	return NewWithClock(cfg, time.Now)
}

// NewWithClock creates a CircuitBreaker using an injectable clock, so tests
// can simulate cooldown elapsing without sleeping.
func NewWithClock(cfg Config, now func() time.Time) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 1
	}
	// See the HalfOpenMaxCalls doc comment: v1 always hard-caps this to 1.
	cfg.HalfOpenMaxCalls = 1
	return &CircuitBreaker{cfg: cfg, now: now, state: Closed}
}

// State returns the breaker's current state, transitioning OPEN -> HALF-OPEN
// as a side effect if the cooldown has elapsed.
func (b *CircuitBreaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked()
}

func (b *CircuitBreaker) stateLocked() State {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		b.state = HalfOpen
		b.halfOpenInFlight = 0
	}
	return b.state
}

// Allow reports whether a call may proceed. In HALF-OPEN it reserves one of
// the limited probe slots; callers that get true must follow up with
// RecordSuccess or RecordFailure.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.stateLocked() {
	case Closed:
		return true
	case HalfOpen:
		if b.halfOpenInFlight >= b.cfg.HalfOpenMaxCalls {
			return false
		}
		b.halfOpenInFlight++
		return true
	default: // Open
		return false
	}
}

// RecordSuccess reports a successful call. In HALF-OPEN this closes the
// circuit; in CLOSED it resets the consecutive failure count.
func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.state = Closed
		b.consecutiveFails = 0
		b.halfOpenInFlight = 0
	case Closed:
		b.consecutiveFails = 0
	}
}

// RecordFailure reports a failed call. In HALF-OPEN this immediately
// reopens the circuit; in CLOSED it increments the failure count and trips
// the breaker once FailureThreshold is reached.
func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.state = Open
		b.openedAt = b.now()
		b.halfOpenInFlight = 0
	case Closed:
		b.consecutiveFails++
		if b.consecutiveFails >= b.cfg.FailureThreshold {
			b.state = Open
			b.openedAt = b.now()
		}
	}
}
