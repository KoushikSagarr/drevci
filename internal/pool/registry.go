package pool

import (
	"context"
	"errors"
	"sync"
	"time"
)

// DefaultPendingCancelTTL is the default lifetime of an early cancellation intent
// before it is evicted if no worker registers the run.
const DefaultPendingCancelTTL = 60 * time.Second

var (
	// ErrAlreadyRegistered is returned when a run ID is already actively registered.
	ErrAlreadyRegistered = errors.New("run is already actively registered")

	// ErrNilCancelFunc is returned when Register is called with a nil cancel function.
	ErrNilCancelFunc = errors.New("cancel function cannot be nil")

	// ErrEmptyRunID is returned when an operation is called with an empty run ID.
	ErrEmptyRunID = errors.New("runID cannot be empty")
)

type pendingCancel struct {
	cause error
	timer *time.Timer
}

// ExecutionRegistry coordinates cancellation signals between API callers and active worker executions.
type ExecutionRegistry struct {
	mu             sync.Mutex
	active         map[string]context.CancelCauseFunc
	pendingCancels map[string]*pendingCancel
	ttl            time.Duration
}

// NewExecutionRegistry creates a new ExecutionRegistry with the default 60-second TTL.
func NewExecutionRegistry() *ExecutionRegistry {
	return NewExecutionRegistryWithTTL(DefaultPendingCancelTTL)
}

// NewExecutionRegistryWithTTL creates a new ExecutionRegistry with a custom TTL for pending cancellations.
func NewExecutionRegistryWithTTL(ttl time.Duration) *ExecutionRegistry {
	if ttl <= 0 {
		ttl = DefaultPendingCancelTTL
	}
	return &ExecutionRegistry{
		active:         make(map[string]context.CancelCauseFunc),
		pendingCancels: make(map[string]*pendingCancel),
		ttl:            ttl,
	}
}

// Register registers an active execution for runID with its cancellation function.
// If an early cancellation intent was already recorded for runID, the pending intent is cleared,
// the run is stored as active, cancel is invoked immediately with the pending cause, and
// Register returns alreadyCancelled = true, err = nil.
// If runID is already actively registered, Register returns false, ErrAlreadyRegistered.
func (r *ExecutionRegistry) Register(runID string, cancel context.CancelCauseFunc) (bool, error) {
	if runID == "" {
		return false, ErrEmptyRunID
	}
	if cancel == nil {
		return false, ErrNilCancelFunc
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.active[runID]; exists {
		return false, ErrAlreadyRegistered
	}

	if pc, ok := r.pendingCancels[runID]; ok {
		pc.timer.Stop()
		delete(r.pendingCancels, runID)
		r.active[runID] = cancel
		cancel(pc.cause)
		return true, nil // Already cancelled
	}

	r.active[runID] = cancel
	return false, nil
}

// Unregister removes runID from the active executions and clears any pending cancellation intent.
// Calling Unregister on an unknown or already-unregistered runID is safe and idempotent.
func (r *ExecutionRegistry) Unregister(runID string) {
	if runID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.active, runID)
	if pc, ok := r.pendingCancels[runID]; ok {
		pc.timer.Stop()
		delete(r.pendingCancels, runID)
	}
}

// Cancel cancels the execution of runID with the given cause.
// If the run is currently active, its cancellation function is invoked immediately and Cancel returns dispatched = true, err = nil.
// If the run is not active, an early cancellation intent is recorded with TTL:
//   - If an intent was already recorded, it returns true, nil.
//   - Otherwise, it schedules a timer to evict the intent after TTL and returns false, nil.
func (r *ExecutionRegistry) Cancel(runID string, cause error) (bool, error) {
	if runID == "" {
		return false, ErrEmptyRunID
	}
	if cause == nil {
		cause = context.Canceled
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if cancel, ok := r.active[runID]; ok {
		cancel(cause)
		return true, nil
	}

	if _, ok := r.pendingCancels[runID]; ok {
		return true, nil // Already recorded
	}

	// Record early intent with TTL
	var timer *time.Timer
	timer = time.AfterFunc(r.ttl, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if pc, ok := r.pendingCancels[runID]; ok && pc.timer == timer {
			delete(r.pendingCancels, runID)
		}
	})

	r.pendingCancels[runID] = &pendingCancel{
		cause: cause,
		timer: timer,
	}
	return false, nil
}

// IsActive reports whether runID is actively registered.
func (r *ExecutionRegistry) IsActive(runID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.active[runID]
	return ok
}

// IsPendingCancel reports whether runID has an unconsumed pending cancellation intent.
func (r *ExecutionRegistry) IsPendingCancel(runID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pendingCancels[runID]
	return ok
}

// ActiveCount returns the number of actively registered runs.
func (r *ExecutionRegistry) ActiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.active)
}

// PendingCount returns the number of pending cancellation intents.
func (r *ExecutionRegistry) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pendingCancels)
}
