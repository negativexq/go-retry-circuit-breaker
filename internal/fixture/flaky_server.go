// Package fixture provides a deterministic, in-process HTTP server for
// exercising retry and circuit-breaker behavior in tests and the demo.
package fixture

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"
)

// NewFlakyServer starts an httptest.Server exposing:
//
//   - GET /fail-first?n=N&key=K
//     Fails with 503 for the first N calls sharing key K, then returns 200.
//     key defaults to "default" if omitted; use distinct keys per test/call
//     site to get independent counters against a single shared server.
//
//   - GET /slow?delay=D
//     Sleeps for duration D (a Go duration string, e.g. "500ms") before
//     responding 200, or returns early if the request context is canceled.
//     Useful for timeout tests. Defaults to 500ms if delay is omitted or
//     invalid.
//
//   - GET /always-fail
//     Always responds 503. Useful for circuit breaker tests.
//
// The caller is responsible for closing the returned server.
func NewFlakyServer() *httptest.Server {
	var mu sync.Mutex
	counts := make(map[string]int)

	mux := http.NewServeMux()

	mux.HandleFunc("/fail-first", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		key := r.URL.Query().Get("key")
		if key == "" {
			key = "default"
		}

		mu.Lock()
		counts[key]++
		call := counts[key]
		mu.Unlock()

		if call <= n {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok on call %d (failed first %d)", call, n)
	})

	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		d, err := time.ParseDuration(r.URL.Query().Get("delay"))
		if err != nil {
			d = 500 * time.Millisecond
		}
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/always-fail", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	return httptest.NewServer(mux)
}
