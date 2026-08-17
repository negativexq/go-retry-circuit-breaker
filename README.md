# go-retry-circuit-breaker

A dependency-free Go resilience lab demonstrating bounded retries, exponential
backoff with jitter, and a concurrency-safe circuit breaker against flaky
HTTP upstreams.

This is not a "retry util library." It's a small, deliberately-scoped lab for
one question: **when an upstream service degrades, how does a well-behaved
client survive that without making things worse?**

```
Client
  |
  v
Retry Policy        <- exponential backoff + jitter, bounded by attempts and deadline
  |
  v
Circuit Breaker      <- fail fast once the upstream is clearly unhealthy
  |
  v
Unreliable Upstream   <- 500s, timeouts, connection refused
```

## Problem

A real upstream doesn't just work or not work — it degrades. It returns
`500`, times out, refuses connections, or gets slow. A naive client either:

- gives up on the first blip (fragile), or
- retries forever with no backoff (amplifies the outage), or
- retries forever with backoff but never stops calling a dead upstream
  (still amplifies it, just more slowly).

This repo builds the layer that sits between "call it once" and "hammer it
forever": bounded retries with jittered backoff, wrapped by a circuit breaker
that stops calling out entirely once the upstream is clearly down.

## Failure Amplification

```
Without retries:
  100 requests -> 100 upstream calls

Naive retries (5 attempts, no breaker):
  100 requests x 5 attempts -> up to 500 upstream calls
  (right when the upstream is already struggling)

With a circuit breaker:
  upstream starts failing
    -> failure threshold reached
    -> circuit opens
    -> client fails fast, upstream gets no more traffic from this client
    -> after a cooldown, a single probe checks if it's back
```

The breaker exists specifically to cut off the amplification retries would
otherwise cause during a sustained outage.

## Retry Policy

```go
type Policy struct {
    MaxAttempts int
    BaseDelay   time.Duration
    MaxDelay    time.Duration
    Jitter      float64 // e.g. 0.2 = +/-20%
}
```

Example configuration used throughout the tests and demo:

```
MaxAttempts = 4
BaseDelay   = 100ms
MaxDelay    = 2s
Jitter      = 0.2 (+/-20%)
```

## Exponential Backoff + Jitter

```
attempt 1 -> immediate
attempt 2 -> ~100ms   (BaseDelay * 2^0)
attempt 3 -> ~200ms   (BaseDelay * 2^1)
attempt 4 -> ~400ms   (BaseDelay * 2^2)
```

capped at `MaxDelay`, then jittered by `+/- Jitter` fraction.

Jitter is not optional in this repo. Without it:

```
100 clients fail at once
  -> all sleep exactly 500ms
  -> all retry at the exact same instant
  -> retry storm hits the upstream right as it's recovering
```

With jitter, the same 100 clients spread their retries across a window
(`430ms`, `512ms`, `548ms`, ...) instead of synchronizing on it. This is a
small detail with a real distributed-systems consequence, so it's exercised
directly in [`retry_test.go`](internal/retry/retry_test.go) with an
injectable random source rather than asserted only indirectly.

## Retryable vs Non-Retryable Failures

```
Retry:
  connection errors, timeouts
  HTTP 429, 502, 503, 504

Do not retry:
  HTTP 400, 401, 403, 404, 409, 422
```

The default classifier (`retry.DefaultRetryable`) also retries on any other
`5xx`, including plain `500`. That's a deliberate **policy choice**, not a
universal truth: a `500` can mean "transient blip" or "this exact request
will never succeed" depending on the upstream, and there's no way to tell
from the status code alone. Treating all `5xx` as retryable is the safer
default for an unreliable upstream and is what this repo's fixture server
and tests assume; swap in a stricter `retry.ClassifyFunc` via
`client.WithClassifier` if your upstream's `500`s are known to be permanent.

## Circuit Breaker

```
type Config struct {
    FailureThreshold int
    OpenTimeout      time.Duration
    HalfOpenMaxCalls int
}
```

Example:

```
5 consecutive failures -> circuit OPEN
open for 3s             -> HALF-OPEN
1 probe succeeds         -> CLOSED
```

## State Machine

```
CLOSED
  |
  | failures >= FailureThreshold
  v
OPEN
  |
  | OpenTimeout elapsed
  v
HALF-OPEN
  |
  +-- success -> CLOSED
  |
  +-- failure -> OPEN
```

The breaker's public surface is intentionally domain-neutral and
state-visible rather than a single `Execute` wrapper:

```go
func (b *CircuitBreaker) Allow() bool
func (b *CircuitBreaker) RecordSuccess()
func (b *CircuitBreaker) RecordFailure()
```

`Allow` also handles the OPEN -> HALF-OPEN transition. `FailureThreshold <= 0`
is normalized to `1` rather than tripping on the very first failure by
accident. `HalfOpenMaxCalls` is reserved for future use — v1 always hard-caps
HALF-OPEN to a single in-flight probe, because allowing multiple concurrent
probes raises an ordering question (a probe that succeeds and closes the
circuit, followed by a still-in-flight probe that then fails, would
incorrectly count as a CLOSED-state failure) that's out of scope here. Time
is injectable (`breaker.NewWithClock`) so tests can advance the cooldown
without sleeping.

## Retry + Breaker Interaction

This is the main design decision in this repo, so it's stated explicitly:

**The breaker guards the logical operation, not each individual HTTP call.**

```
1 logical operation (client.Get)
  -> up to MaxAttempts retry attempts against the upstream

  all attempts fail  -> breaker records exactly 1 failure
  any attempt succeeds -> breaker records exactly 1 success
```

If every attempt inside one retry loop counted as its own breaker failure,
a single slow degradation would trip the breaker almost immediately and in
proportion to `MaxAttempts` rather than to actual distinct failed
operations. Counting the aggregate outcome once keeps `FailureThreshold`
meaning what it says: N failed *operations*, not N failed *HTTP calls*.

One more refinement in [`internal/client`](internal/client/client.go): a
non-retryable client error (e.g. `400`) is **not** reported to the breaker
at all. It's not a signal about upstream health — it means the upstream
responded and rejected this specific request. Only an exhausted-retries
failure or a genuine success move the breaker's state.

## Run

```bash
go run ./cmd/demo
```

Runs three scenarios against an in-process flaky server: eventual success
after transient `503`s, a sustained outage that trips the breaker, and
recovery once the cooldown elapses and a half-open probe succeeds.

## Tests

```bash
make test   # go test ./...
make race   # go test -race ./...
make ci     # build + vet + test + race
```

Star tests:

- `TestRetryEventuallySucceeds` — `/fail-first?n=3` then `200`; asserts 4
  attempts and success.
- `TestDoesNotRetryOn400` — asserts exactly 1 attempt and 1 upstream call.
- `TestRetryStopsOnContextCancellation` — 1s backoff vs. a 100ms context
  deadline; asserts the call returns in well under 1s.
- `TestCircuitBreakerOpensAfterThreshold` — asserts both `ErrCircuitOpen`
  *and* that the upstream call count stops increasing once OPEN.
- `TestHalfOpenRecoversToClosedOnProbeSuccess` — OPEN -> (injected clock
  advance) -> HALF-OPEN -> successful probe -> CLOSED, no real sleeping.
- `TestJitterProducesVariedBoundedDelays` — injectable RNG shows computed
  delays vary call-to-call while staying within the jitter bounds.

The flaky upstream used by these tests is
[`internal/fixture/flaky_server.go`](internal/fixture/flaky_server.go):
`/fail-first?n=N&key=K`, `/slow?delay=D`, and `/always-fail`.

## Failure Semantics

- Retries are bounded by **both** `MaxAttempts` and the caller's context
  deadline — whichever is hit first stops the loop. There is no unbounded
  retry path.
- Sleeps between attempts are context-aware (`select` on `time.After` and
  `ctx.Done()`), so a canceled/expired context interrupts a pending backoff
  immediately instead of waiting it out.
- If the upstream sends `Retry-After` (integer seconds only, no HTTP-date
  support in v1) on a `429`/`503`, it overrides the computed backoff delay
  for the next attempt.
- Only `GET` is implemented. Retrying `POST`/other non-idempotent methods
  safely is a separate correctness problem (idempotency keys, dedup) that's
  out of scope here — see the idempotency-focused repo in this series
  instead.
- Response body ownership: `retry.Do` drains and closes every intermediate
  attempt's response body internally (so a retried `503`/`500` doesn't leak
  a connection), and hands back only the final `*http.Response` — that one
  the caller owns and must close. See
  [`TestRetryClosesIntermediateResponseBodies`](internal/retry/retry_test.go).

## Engineering Notes

Deliberately out of scope for v1:

```
bulkhead isolation
hedged requests
adaptive concurrency
distributed / shared circuit breaker state (e.g. Redis-backed)
metrics exporters, OpenTelemetry
service discovery
gRPC / multi-protocol support
POST retries
```

Stdlib-only, no external dependencies.

## Series

```
go-api-prober        -> observe pressure
go-idempotency-lab    -> preserve correctness
go-rate-limiter       -> control pressure
go-retry-circuit-breaker -> survive failure
```

## License

MIT — see [LICENSE](LICENSE).
