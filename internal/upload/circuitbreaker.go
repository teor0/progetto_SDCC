package upload

import (
	"errors"
	"log"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the circuit breaker is open and calls are
// being rejected to protect the downstream dependency.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// state represents the three states of the circuit breaker.
type state int

const (
	stateClosed   state = iota // normal operation — calls pass through
	stateOpen                  // dependency unhealthy — calls are rejected
	stateHalfOpen              // probe state — one call allowed to test recovery
)

func (s state) String() string {
	switch s {
	case stateClosed:
		return "CLOSED"
	case stateOpen:
		return "OPEN"
	case stateHalfOpen:
		return "HALF-OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreaker is a thread-safe three-state circuit breaker.
// Transitions:
//
//	CLOSED  → OPEN      after consecutiveFailures >= maxFailures
//	OPEN    → HALF-OPEN after resetTimeout elapses
//	HALF-OPEN → CLOSED  on a successful probe call
//	HALF-OPEN → OPEN    on a failed probe call
type CircuitBreaker struct {
	mu                  sync.Mutex
	current             state
	consecutiveFailures int
	lastFailure         time.Time
	probing             bool // true for the full duration of an in-flight half-open probe

	maxFailures  int           // failures before opening
	resetTimeout time.Duration // how long to stay open before probing
}

// NewCircuitBreaker returns a breaker that opens after maxFailures consecutive
// failures and attempts a probe after resetTimeout.
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		current:      stateClosed,
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
	}
}

// Call executes fn if the circuit is closed, or if it's half-open and no
// probe is currently in flight. It records success/failure and transitions
// state accordingly.
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()

	switch cb.current {
	case stateOpen:
		if time.Since(cb.lastFailure) >= cb.resetTimeout {
			// Enough time has passed — allow exactly one probe call, and
			// mark it in-flight so nobody else can piggyback on it while
			// fn() is running outside the lock below.
			cb.current = stateHalfOpen
			cb.probing = true
			log.Printf("CircuitBreaker → %s (probing)", cb.current)
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}

	case stateHalfOpen:
		if cb.probing {
			// A probe is already in flight — reject instead of piling on
			// a dependency that's still being tested for recovery.
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.probing = true
	}

	cb.mu.Unlock()

	// Execute the call outside the lock so we do not block other goroutines
	// for the duration of the network call.
	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.probing = false

	if err != nil {
		cb.consecutiveFailures++
		cb.lastFailure = time.Now()

		if cb.current == stateHalfOpen || cb.consecutiveFailures >= cb.maxFailures {
			cb.current = stateOpen
			log.Printf("CircuitBreaker → %s (failures=%d)", cb.current, cb.consecutiveFailures)
		}
		return err
	}

	// Success — reset to closed regardless of previous state.
	if cb.current != stateClosed {
		log.Printf("CircuitBreaker → %s (recovered)", stateClosed)
	}
	cb.current = stateClosed
	cb.consecutiveFailures = 0
	return nil
}

// State returns the current circuit breaker state (for logging / metrics).
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.current.String()
}
