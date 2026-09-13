package upload

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

//run with:
//go test ./internal/upload/... -run TestCircuitBreaker -v

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

	// Confirm the failure count actually reset
	for range maxFailures - 1 {
		_ = cb.Call(func() error { return errBoom })
	}
	if got := cb.State(); got != "CLOSED" {
		t.Fatalf("expected still CLOSED after %d failures (below threshold), got %s", maxFailures-1, got)
	}
}
