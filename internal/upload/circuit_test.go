package upload

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

//run with:
//go test ./internal/upload/... -run TestCircuitBreaker -v -race

func TestCircuitBreaker_StartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", got)
	}
}

func TestCircuitBreaker_StaysClosedOnRepeatedSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	for i := range 10 {
		if err := cb.Call(func() error { return nil }); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", got)
	}
}

func TestCircuitBreaker_OpensAfterMaxConsecutiveFailures(t *testing.T) {
	const maxFailures = 3
	cb := NewCircuitBreaker(maxFailures, 50*time.Millisecond)

	for i := range maxFailures {
		err := cb.Call(func() error { return errBoom })
		if !errors.Is(err, errBoom) {
			t.Fatalf("call %d: expected errBoom, got %v", i, err)
		}
	}

	if got := cb.State(); got != "OPEN" {
		t.Fatalf("expected OPEN after %d consecutive failures, got %s", maxFailures, got)
	}
}

func TestCircuitBreaker_RejectsWithoutCallingFnWhileOpen(t *testing.T) {
	const maxFailures = 2
	// Long timeout guarantees the breaker is still OPEN by the time we probe it below.
	cb := NewCircuitBreaker(maxFailures, time.Hour)

	for range maxFailures {
		_ = cb.Call(func() error { return errBoom })
	}
	if got := cb.State(); got != "OPEN" {
		t.Fatalf("setup: expected OPEN, got %s", got)
	}

	var called int32
	err := cb.Call(func() error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if atomic.LoadInt32(&called) != 0 {
		t.Fatal("fn should not have been invoked while the circuit is open")
	}
}

func TestCircuitBreaker_ProbesAfterResetTimeoutElapses(t *testing.T) {
	const maxFailures = 2
	resetTimeout := 30 * time.Millisecond
	cb := NewCircuitBreaker(maxFailures, resetTimeout)

	for range maxFailures {
		_ = cb.Call(func() error { return errBoom })
	}

	time.Sleep(resetTimeout + 20*time.Millisecond)

	var called int32
	_ = cb.Call(func() error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	if atomic.LoadInt32(&called) != 1 {
		t.Fatal("expected the probe call to actually invoke fn once the reset timeout elapsed")
	}
}

func TestCircuitBreaker_SuccessfulProbeClosesCircuit(t *testing.T) {
	const maxFailures = 2
	resetTimeout := 30 * time.Millisecond
	cb := NewCircuitBreaker(maxFailures, resetTimeout)

	for range maxFailures {
		_ = cb.Call(func() error { return errBoom })
	}
	time.Sleep(resetTimeout + 20*time.Millisecond)

	if err := cb.Call(func() error { return nil }); err != nil {
		t.Fatalf("probe call: unexpected error: %v", err)
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected CLOSED after a successful probe, got %s", got)
	}

	// Confirm the failure count actually reset, not just the state label:
	// it should take a fresh run of maxFailures failures to reopen.
	for range maxFailures - 1 {
		_ = cb.Call(func() error { return errBoom })
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected still CLOSED after %d failures (below threshold), got %s", maxFailures-1, got)
	}
}

func TestCircuitBreaker_FailedProbeReopensCircuit(t *testing.T) {
	const maxFailures = 2
	resetTimeout := 30 * time.Millisecond
	cb := NewCircuitBreaker(maxFailures, resetTimeout)

	for range maxFailures {
		_ = cb.Call(func() error { return errBoom })
	}
	time.Sleep(resetTimeout + 20*time.Millisecond)

	if err := cb.Call(func() error { return errBoom }); !errors.Is(err, errBoom) {
		t.Fatalf("probe call: expected errBoom, got %v", err)
	}
	if got := cb.State(); got != "OPEN" {
		t.Fatalf("expected OPEN again after a failed probe, got %s", got)
	}
}

func TestCircuitBreaker_InterveningSuccessResetsFailureCount(t *testing.T) {
	const maxFailures = 3
	cb := NewCircuitBreaker(maxFailures, time.Hour)

	// maxFailures-1 failures alone should not open the circuit...
	for range maxFailures - 1 {
		_ = cb.Call(func() error { return errBoom })
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected still CLOSED, got %s", got)
	}

	// ...a success in between resets the streak...
	if err := cb.Call(func() error { return nil }); err != nil {
		t.Fatalf("unexpected error on success: %v", err)
	}

	// ...so maxFailures-1 more failures right after should NOT be enough
	// to open it -- if the streak weren't reset, this would tip it over.
	for range maxFailures - 1 {
		_ = cb.Call(func() error { return errBoom })
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected CLOSED (streak should have been reset by the intervening success), got %s", got)
	}
}

// TestCircuitBreaker_OnlyOneConcurrentProbe is the regression test for the
// half-open exclusivity fix: once the breaker becomes eligible to probe,
// many goroutines calling Call() at once must result in exactly one of them
// actually invoking fn -- the rest must be rejected immediately rather than
// piling onto a dependency that's still being tested for recovery.
//
// Run with -race; the original bug (unlocking before fn() ran, so a second
// goroutine could observe stateHalfOpen and slip through) is exactly the
// kind of thing -race is good at surfacing even beyond the count assertion.
func TestCircuitBreaker_OnlyOneConcurrentProbe(t *testing.T) {
	const maxFailures = 1
	resetTimeout := 30 * time.Millisecond
	cb := NewCircuitBreaker(maxFailures, resetTimeout)

	// Trip the breaker.
	_ = cb.Call(func() error { return errBoom })
	if got := cb.State(); got != "OPEN" {
		t.Fatalf("setup: expected OPEN, got %s", got)
	}

	time.Sleep(resetTimeout + 20*time.Millisecond)

	const numGoroutines = 50
	var (
		wg          sync.WaitGroup
		probeCount  int32
		rejectCount int32
		blockProbe  = make(chan struct{})
	)

	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			err := cb.Call(func() error {
				atomic.AddInt32(&probeCount, 1)
				// Hold the probe open. If the exclusivity fix regressed,
				// other goroutines would have this whole window to sneak
				// in and increment probeCount too.
				<-blockProbe
				return nil
			})
			if errors.Is(err, ErrCircuitOpen) {
				atomic.AddInt32(&rejectCount, 1)
			}
		}()
	}

	// Give every goroutine a chance to reach cb.Call before releasing the
	// one probe that got in.
	time.Sleep(20 * time.Millisecond)
	close(blockProbe)
	wg.Wait()

	if got := atomic.LoadInt32(&probeCount); got != 1 {
		t.Fatalf("expected exactly 1 goroutine to run the probe, got %d", got)
	}
	if got := atomic.LoadInt32(&rejectCount); got != numGoroutines-1 {
		t.Fatalf("expected %d rejections, got %d", numGoroutines-1, got)
	}
}
