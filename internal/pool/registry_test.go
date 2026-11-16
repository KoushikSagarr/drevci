package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// 1. Register → Cancel → Unregister lifecycle
func TestExecutionRegistry_RegisterCancelUnregister_Lifecycle(t *testing.T) {
	reg := NewExecutionRegistry()
	runID := "run-lifecycle-1"

	ctx, cancel := context.WithCancelCause(context.Background())
	var cancelCalled bool
	var receivedCause error

	wrappedCancel := func(cause error) {
		cancelCalled = true
		receivedCause = cause
		cancel(cause)
	}

	// 1. Register active run
	alreadyCancelled, err := reg.Register(runID, wrappedCancel)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if alreadyCancelled {
		t.Errorf("expected alreadyCancelled = false, got true")
	}
	if !reg.IsActive(runID) {
		t.Errorf("expected run %s to be active", runID)
	}
	if reg.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", reg.ActiveCount())
	}
	if reg.IsPendingCancel(runID) {
		t.Errorf("expected IsPendingCancel = false")
	}

	// 2. Cancel active run
	expectedCause := errors.New("user requested abort")
	dispatched, err := reg.Cancel(runID, expectedCause)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if !dispatched {
		t.Errorf("expected dispatched = true for active run")
	}
	if !cancelCalled {
		t.Errorf("cancel function was not called")
	}
	if !errors.Is(receivedCause, expectedCause) {
		t.Errorf("receivedCause = %v, want %v", receivedCause, expectedCause)
	}
	if !errors.Is(context.Cause(ctx), expectedCause) {
		t.Errorf("context.Cause(ctx) = %v, want %v", context.Cause(ctx), expectedCause)
	}

	// 3. Unregister
	reg.Unregister(runID)
	if reg.IsActive(runID) {
		t.Errorf("run %s still active after Unregister", runID)
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}
}

// 2. Cancel before Register
func TestExecutionRegistry_CancelBeforeRegister(t *testing.T) {
	reg := NewExecutionRegistry()
	runID := "run-cancel-before-register"
	customCause := errors.New("early cancel intent")

	// 1. Cancel arrives while worker is dequeuing
	dispatched, err := reg.Cancel(runID, customCause)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if dispatched {
		t.Errorf("expected dispatched = false when canceling unstarted run")
	}
	if !reg.IsPendingCancel(runID) {
		t.Errorf("expected IsPendingCancel = true")
	}
	if reg.PendingCount() != 1 {
		t.Errorf("PendingCount = %d, want 1", reg.PendingCount())
	}
	if reg.IsActive(runID) {
		t.Errorf("expected IsActive = false")
	}

	// 2. Worker calls Register later
	var cancelCalled bool
	var receivedCause error
	cancelFunc := func(cause error) {
		cancelCalled = true
		receivedCause = cause
	}

	alreadyCancelled, err := reg.Register(runID, cancelFunc)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !alreadyCancelled {
		t.Errorf("expected alreadyCancelled = true")
	}
	if !cancelCalled {
		t.Errorf("cancel function was not immediately called on Register")
	}
	if !errors.Is(receivedCause, customCause) {
		t.Errorf("receivedCause = %v, want %v", receivedCause, customCause)
	}

	// 3. Verify pending intent was consumed and run is marked active until Unregister
	if reg.IsPendingCancel(runID) {
		t.Errorf("pending cancel was not removed from registry")
	}
	if reg.PendingCount() != 0 {
		t.Errorf("PendingCount = %d, want 0", reg.PendingCount())
	}
	if !reg.IsActive(runID) {
		t.Errorf("expected run %s to be actively registered after Register consumed early cancel", runID)
	}
	if reg.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", reg.ActiveCount())
	}

	// 4. Duplicate Register before Unregister must return ErrAlreadyRegistered
	_, err = reg.Register(runID, func(cause error) {})
	if !errors.Is(err, ErrAlreadyRegistered) {
		t.Errorf("duplicate Register before unregister: err = %v, want %v", err, ErrAlreadyRegistered)
	}

	// 5. Unregister removes active registration
	reg.Unregister(runID)
	if reg.IsActive(runID) {
		t.Errorf("run %s still active after Unregister", runID)
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}

	// 6. Registration succeeds again after Unregister
	alreadyCancelled2, err := reg.Register(runID, func(cause error) {})
	if err != nil || alreadyCancelled2 {
		t.Fatalf("Register after Unregister: alreadyCancelled=%v, err=%v", alreadyCancelled2, err)
	}
	reg.Unregister(runID)
}

// 3. Repeated cancellation
func TestExecutionRegistry_RepeatedCancellation(t *testing.T) {
	reg := NewExecutionRegistry()

	t.Run("active run repeated cancel", func(t *testing.T) {
		runID := "run-repeat-active"
		callCount := 0
		cancelFunc := func(cause error) {
			callCount++
		}

		if _, err := reg.Register(runID, cancelFunc); err != nil {
			t.Fatalf("Register: %v", err)
		}

		// First cancel
		disp1, err := reg.Cancel(runID, errors.New("first cancel"))
		if err != nil || !disp1 {
			t.Fatalf("first cancel: disp=%v, err=%v", disp1, err)
		}
		if callCount != 1 {
			t.Errorf("callCount = %d, want 1", callCount)
		}

		// Second cancel (duplicate)
		disp2, err := reg.Cancel(runID, errors.New("second cancel"))
		if err != nil || !disp2 {
			t.Fatalf("second cancel: disp=%v, err=%v", disp2, err)
		}
		if callCount != 2 {
			t.Errorf("callCount = %d, want 2 (cancel func invoked)", callCount)
		}

		reg.Unregister(runID)
	})

	t.Run("pending cancel repeated cancel", func(t *testing.T) {
		runID := "run-repeat-pending"
		cause1 := errors.New("early cancel 1")
		cause2 := errors.New("early cancel 2")

		// First cancel: records early intent
		disp1, err := reg.Cancel(runID, cause1)
		if err != nil || disp1 {
			t.Fatalf("first cancel: disp=%v, err=%v", disp1, err)
		}
		if !reg.IsPendingCancel(runID) {
			t.Errorf("expected pending cancel to be recorded")
		}

		// Second cancel: idempotent safe no-op returning true
		disp2, err := reg.Cancel(runID, cause2)
		if err != nil || !disp2 {
			t.Fatalf("second cancel: disp=%v, err=%v", disp2, err)
		}
		if reg.PendingCount() != 1 {
			t.Errorf("PendingCount = %d, want 1", reg.PendingCount())
		}

		// Consume by register: must receive the original intent
		var receivedCause error
		alreadyCancelled, err := reg.Register(runID, func(cause error) {
			receivedCause = cause
		})
		if err != nil || !alreadyCancelled {
			t.Fatalf("Register: alreadyCancelled=%v, err=%v", alreadyCancelled, err)
		}
		if !errors.Is(receivedCause, cause1) {
			t.Errorf("receivedCause = %v, want %v", receivedCause, cause1)
		}
		reg.Unregister(runID)
	})
}

// 4. Repeated registration behavior
func TestExecutionRegistry_RepeatedRegistration(t *testing.T) {
	reg := NewExecutionRegistry()
	runID := "run-repeat-reg"

	cancel1 := func(cause error) {}
	cancel2 := func(cause error) {}

	// First registration succeeds
	alreadyCancelled, err := reg.Register(runID, cancel1)
	if err != nil || alreadyCancelled {
		t.Fatalf("first Register failed: %v", err)
	}

	// Second registration of same active ID fails with ErrAlreadyRegistered
	_, err = reg.Register(runID, cancel2)
	if !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("second Register err = %v, want %v", err, ErrAlreadyRegistered)
	}

	// Unregister clears active entry
	reg.Unregister(runID)

	// Third registration now succeeds
	cancel3 := func(cause error) {}
	alreadyCancelled3, err := reg.Register(runID, cancel3)
	if err != nil || alreadyCancelled3 {
		t.Fatalf("third Register after unregister failed: %v", err)
	}
	reg.Unregister(runID)
}

// 5. Unregister of unknown IDs
func TestExecutionRegistry_UnregisterUnknownIDs(t *testing.T) {
	reg := NewExecutionRegistry()

	// Unregister non-existent run ID is a safe no-op
	reg.Unregister("non-existent-run-id")
	reg.Unregister("another-unknown")
	reg.Unregister("")

	if reg.ActiveCount() != 0 || reg.PendingCount() != 0 {
		t.Errorf("unexpected counts: active=%d, pending=%d", reg.ActiveCount(), reg.PendingCount())
	}
}

// 6. Pending cancellation expiration and cleanup
func TestExecutionRegistry_PendingCancellationExpiration(t *testing.T) {
	// Configure short TTL for test
	shortTTL := 30 * time.Millisecond
	reg := NewExecutionRegistryWithTTL(shortTTL)
	runID := "run-ttl-expiration"

	// 1. Record pending cancel
	disp, err := reg.Cancel(runID, errors.New("timeout cancel"))
	if err != nil || disp {
		t.Fatalf("Cancel failed: %v", err)
	}
	if !reg.IsPendingCancel(runID) {
		t.Fatalf("expected pending cancel recorded")
	}

	// 2. Wait for TTL to expire
	time.Sleep(shortTTL * 3)

	// 3. Verify entry was evicted by timer
	if reg.IsPendingCancel(runID) {
		t.Errorf("pending cancel was not evicted after TTL expired")
	}
	if reg.PendingCount() != 0 {
		t.Errorf("PendingCount = %d, want 0", reg.PendingCount())
	}

	// 4. Register after eviction: behaves as normal uncancelled registration
	var cancelCalled bool
	alreadyCancelled, err := reg.Register(runID, func(cause error) {
		cancelCalled = true
	})
	if err != nil {
		t.Fatalf("Register after expiration failed: %v", err)
	}
	if alreadyCancelled {
		t.Errorf("expected alreadyCancelled = false after expiration, got true")
	}
	if cancelCalled {
		t.Errorf("cancel func was invoked despite intent expiration")
	}
	if !reg.IsActive(runID) {
		t.Errorf("run should be actively registered")
	}

	reg.Unregister(runID)
}

// 7. Input validation
func TestExecutionRegistry_InputValidation(t *testing.T) {
	reg := NewExecutionRegistry()

	// Empty runID in Register
	_, err := reg.Register("", func(cause error) {})
	if !errors.Is(err, ErrEmptyRunID) {
		t.Errorf("Register empty runID: err = %v, want %v", err, ErrEmptyRunID)
	}

	// Nil cancel func in Register
	_, err = reg.Register("run-nil-cancel", nil)
	if !errors.Is(err, ErrNilCancelFunc) {
		t.Errorf("Register nil cancel: err = %v, want %v", err, ErrNilCancelFunc)
	}

	// Empty runID in Cancel
	_, err = reg.Cancel("", nil)
	if !errors.Is(err, ErrEmptyRunID) {
		t.Errorf("Cancel empty runID: err = %v, want %v", err, ErrEmptyRunID)
	}

	// Nil cause in Cancel defaults to context.Canceled
	var receivedCause error
	_, err = reg.Register("run-nil-cause", func(cause error) {
		receivedCause = cause
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	disp, err := reg.Cancel("run-nil-cause", nil)
	if err != nil || !disp {
		t.Fatalf("Cancel nil cause failed: %v", err)
	}
	if !errors.Is(receivedCause, context.Canceled) {
		t.Errorf("receivedCause = %v, want %v", receivedCause, context.Canceled)
	}
	reg.Unregister("run-nil-cause")
}

// 8. Concurrent operations and race safety
func TestExecutionRegistry_ConcurrentOperations_RaceSafety(t *testing.T) {
	reg := NewExecutionRegistryWithTTL(10 * time.Millisecond)
	const numGoroutines = 50
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for i := 0; i < operationsPerGoroutine; i++ {
				runID := fmt.Sprintf("run-concurrent-%d", rng.Intn(10))
				op := rng.Intn(5)

				switch op {
				case 0:
					// Register
					_, _ = reg.Register(runID, func(cause error) {})
				case 1:
					// Cancel
					_, _ = reg.Cancel(runID, errors.New("concurrent cancel"))
				case 2:
					// Unregister
					reg.Unregister(runID)
				case 3:
					// IsActive
					_ = reg.IsActive(runID)
				case 4:
					// IsPendingCancel
					_ = reg.IsPendingCancel(runID)
				}
			}
		}(g)
	}

	wg.Wait()
}

// 9. Dedicated Early Cancellation Registration Lifecycle test verifying points 1-9
func TestExecutionRegistry_EarlyCancellation_RegistrationLifecycle(t *testing.T) {
	reg := NewExecutionRegistry()
	runID := "run-early-cancel-lifecycle"
	originalCause := errors.New("pipeline canceled by user before execution")

	// 1. Cancel before Register
	disp, err := reg.Cancel(runID, originalCause)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if disp {
		t.Errorf("expected dispatched = false for early cancel")
	}
	if !reg.IsPendingCancel(runID) {
		t.Errorf("expected pending cancel to be recorded")
	}
	if reg.IsActive(runID) {
		t.Errorf("expected run not to be active before Register")
	}

	// 9. Existing repeated cancellation behavior remains correct: repeated cancel while pending
	dispDup, err := reg.Cancel(runID, errors.New("second cancel attempt"))
	if err != nil || !dispDup {
		t.Errorf("expected duplicate cancel on pending run to return (true, nil): disp=%v, err=%v", dispDup, err)
	}

	// 2. Register consumes pending cancellation
	// 3. The cancellation function receives the original cause
	var cancelCalled bool
	var receivedCause error
	cancel1 := func(cause error) {
		cancelCalled = true
		receivedCause = cause
	}

	alreadyCancelled, err := reg.Register(runID, cancel1)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !alreadyCancelled {
		t.Errorf("expected alreadyCancelled = true")
	}
	if !cancelCalled {
		t.Errorf("cancel function was not called on Register")
	}
	if !errors.Is(receivedCause, originalCause) {
		t.Errorf("receivedCause = %v, want %v", receivedCause, originalCause)
	}

	// 8. The pending timer is stopped/consumed correctly (no longer pending)
	if reg.IsPendingCancel(runID) {
		t.Errorf("expected pending cancel to be cleared")
	}
	if reg.PendingCount() != 0 {
		t.Errorf("PendingCount = %d, want 0", reg.PendingCount())
	}

	// 4. The run is reported active after Register returns
	if !reg.IsActive(runID) {
		t.Errorf("expected run %s to be reported active after Register", runID)
	}
	if reg.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", reg.ActiveCount())
	}

	// 5. A second Register before Unregister returns ErrAlreadyRegistered
	cancel2Called := false
	cancel2 := func(cause error) {
		cancel2Called = true
	}
	alreadyCancelled2, err := reg.Register(runID, cancel2)
	if !errors.Is(err, ErrAlreadyRegistered) {
		t.Errorf("second Register before Unregister: err = %v, want %v", err, ErrAlreadyRegistered)
	}
	if alreadyCancelled2 {
		t.Errorf("second Register: expected alreadyCancelled = false on error")
	}
	if cancel2Called {
		t.Errorf("cancel2 should not be called on rejected duplicate registration")
	}

	// 6. Unregister removes the active registration
	reg.Unregister(runID)
	if reg.IsActive(runID) {
		t.Errorf("run %s still active after Unregister", runID)
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}

	// 7. Registration succeeds again after Unregister
	cancel3 := func(cause error) {}
	alreadyCancelled3, err := reg.Register(runID, cancel3)
	if err != nil {
		t.Fatalf("Register after Unregister failed: %v", err)
	}
	if alreadyCancelled3 {
		t.Errorf("expected alreadyCancelled = false on fresh registration after Unregister")
	}
	if !reg.IsActive(runID) {
		t.Errorf("run should be active after fresh registration")
	}

	reg.Unregister(runID)
}

// 10. Concurrency test for duplicate registration attempts after early cancellation
func TestExecutionRegistry_ConcurrentDuplicateRegister_AfterEarlyCancel(t *testing.T) {
	reg := NewExecutionRegistry()
	runID := "run-concurrent-early-cancel"
	originalCause := errors.New("early cancel intent")

	// 1. Record early cancellation
	if _, err := reg.Cancel(runID, originalCause); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	const numWorkers = 25
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	type registerResult struct {
		alreadyCancelled bool
		err              error
		causeReceived    error
	}

	results := make([]registerResult, numWorkers)

	startGate := make(chan struct{})
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			<-startGate // Synchronize launch

			var causeRec error
			cancelFunc := func(c error) {
				causeRec = c
			}

			alreadyCancelled, err := reg.Register(runID, cancelFunc)
			results[workerID] = registerResult{
				alreadyCancelled: alreadyCancelled,
				err:              err,
				causeReceived:    causeRec,
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	// Exactly ONE worker must succeed with (alreadyCancelled = true, err = nil) and receive originalCause
	successfulCount := 0
	alreadyRegisteredCount := 0

	for i, res := range results {
		if res.err == nil {
			if !res.alreadyCancelled {
				t.Errorf("worker %d succeeded but alreadyCancelled was false", i)
			}
			if !errors.Is(res.causeReceived, originalCause) {
				t.Errorf("worker %d received wrong cause: %v, want %v", i, res.causeReceived, originalCause)
			}
			successfulCount++
		} else if errors.Is(res.err, ErrAlreadyRegistered) {
			alreadyRegisteredCount++
		} else {
			t.Errorf("worker %d unexpected error: %v", i, res.err)
		}
	}

	if successfulCount != 1 {
		t.Fatalf("expected exactly 1 worker to register early cancel, got %d", successfulCount)
	}
	if alreadyRegisteredCount != numWorkers-1 {
		t.Fatalf("expected exactly %d workers to get ErrAlreadyRegistered, got %d", numWorkers-1, alreadyRegisteredCount)
	}

	// Run must remain active
	if !reg.IsActive(runID) {
		t.Errorf("expected run %s to remain active", runID)
	}

	// Unregister clears active entry
	reg.Unregister(runID)
	if reg.IsActive(runID) {
		t.Errorf("run %s still active after Unregister", runID)
	}
}
