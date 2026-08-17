// Command demo runs the retry+breaker client against an in-process flaky
// upstream to make the failure-handling behavior visible.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/negativexq/go-retry-circuit-breaker/internal/breaker"
	"github.com/negativexq/go-retry-circuit-breaker/internal/client"
	"github.com/negativexq/go-retry-circuit-breaker/internal/fixture"
	"github.com/negativexq/go-retry-circuit-breaker/internal/retry"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv := fixture.NewFlakyServer()
	defer srv.Close()

	cb := breaker.New(breaker.Config{FailureThreshold: 3, OpenTimeout: 2 * time.Second})
	c := client.New(retry.Policy{
		MaxAttempts: 4,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Jitter:      0.2,
	}, cb, client.WithLogger(logger))

	fmt.Println("=== scenario 1: transient failures, eventual success ===")
	_, result, err := c.Get(context.Background(), srv.URL+"/fail-first?n=3&key=demo1")
	fmt.Printf("-> attempts=%d retried=%v breaker=%s err=%v\n\n", result.Attempts, result.Retried, result.BreakerState, err)

	fmt.Println("=== scenario 2: upstream always failing, breaker trips ===")
	for i := 1; i <= 5; i++ {
		_, result, err := c.Get(context.Background(), srv.URL+"/always-fail")
		fmt.Printf("call %d -> attempts=%d breaker=%s err=%v\n", i, result.Attempts, result.BreakerState, err)
	}

	fmt.Println("\n=== scenario 3: cooldown elapses, half-open probe recovers ===")
	fmt.Println("(waiting for OpenTimeout to elapse...)")
	time.Sleep(2100 * time.Millisecond)
	_, result, err = c.Get(context.Background(), srv.URL+"/fail-first?n=0&key=demo3")
	fmt.Printf("-> attempts=%d breaker=%s err=%v\n", result.Attempts, result.BreakerState, err)
}
