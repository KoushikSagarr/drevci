package pool

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// TestPool_RunScopedContext_RegistryRegistration_Lifecycle verifies:
// 1. A normal dequeued run receives a run-scoped context derived from daemon context.
// 2. Registry registration occurs before scheduler execution.
// 3. Scheduler receives the run-scoped context rather than raw daemon context.
// 4. Registry unregistration occurs after execution.
// 5. No stale registry entries remain after worker finishes.
func TestPool_RunScopedContext_RegistryRegistration_Lifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_lifecycle.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-lifecycle-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-life",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "rj-life-1",
		RunID:     runID,
		JobName:   "life-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	startedCh := make(chan struct{})
	releaseCh := make(chan struct{})

	var capturedCtx context.Context
	var isActiveDuringExecution bool

	q := queue.New(10)
	reg := NewExecutionRegistry()

	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			capturedCtx = runCtx
			isActiveDuringExecution = reg.IsActive(runID)
			close(startedCh)
			<-releaseCh
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-life",
			Jobs: []drevtypes.Job{
				{Name: "life-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step1", Run: "echo hi"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	select {
	case <-startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runner execution to start")
	}

	// Verify registry state while worker is executing
	if !isActiveDuringExecution {
		t.Error("expected run to be actively registered during runner execution")
	}
	if !reg.IsActive(runID) {
		t.Error("expected registry.IsActive(runID) == true while running")
	}
	if reg.ActiveCount() != 1 {
		t.Errorf("expected ActiveCount == 1, got %d", reg.ActiveCount())
	}

	// Verify captured context is derived from daemon context but distinct
	if capturedCtx == daemonCtx {
		t.Error("expected scheduler context to be a run-scoped child context, not the raw daemon context")
	}
	if capturedCtx.Err() != nil {
		t.Errorf("expected captured context to be uncancelled, got %v", capturedCtx.Err())
	}

	// Release worker and wait for completion
	close(releaseCh)

	// Poll until run completes and registry is clean
	deadline := time.After(3 * time.Second)
	for {
		if !reg.IsActive(runID) && reg.ActiveCount() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for registry unregistration; active count = %d", reg.ActiveCount())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if reg.PendingCount() != 0 {
		t.Errorf("expected PendingCount == 0, got %d", reg.PendingCount())
	}

	daemonCancel()
	p.Stop()
}

// TestPool_ActiveCancellation_CancelsSchedulerContext verifies:
// 1. Calling registry.Cancel while execution is active cancels the run-scoped context.
// 2. The cancellation cause propagates accurately to context.Cause(runCtx).
// 3. The raw daemon context remains completely uncancelled.
// 4. Registry unregisters the run after execution completes.
func TestPool_ActiveCancellation_CancelsSchedulerContext(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_active_cancel.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-active-cancel"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-act-cancel",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "rj-act-1",
		RunID:     runID,
		JobName:   "cancel-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	startedCh := make(chan struct{})
	customCause := errors.New("user abort via cancellation API")

	var capturedCause error
	var capturedErr error

	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(startedCh)
			<-runCtx.Done()
			capturedErr = runCtx.Err()
			capturedCause = context.Cause(runCtx)
			return capturedErr
		},
	}

	q := queue.New(10)
	reg := NewExecutionRegistry()
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-cancel",
			Jobs: []drevtypes.Job{
				{Name: "cancel-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "sleep 10"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	select {
	case <-startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runner execution to start")
	}

	// Trigger cancellation via registry
	dispatched, err := reg.Cancel(runID, customCause)
	if err != nil {
		t.Fatalf("reg.Cancel error: %v", err)
	}
	if !dispatched {
		t.Fatal("expected cancel to be immediately dispatched to active run")
	}

	// Wait for worker to finish
	deadline := time.After(3 * time.Second)
	for {
		if !reg.IsActive(runID) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for worker to unregister cancelled run")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if !errors.Is(capturedErr, context.Canceled) {
		t.Errorf("expected capturedErr == context.Canceled, got %v", capturedErr)
	}
	if !errors.Is(capturedCause, customCause) {
		t.Errorf("expected capturedCause == %v, got %v", customCause, capturedCause)
	}
	if daemonCtx.Err() != nil {
		t.Errorf("daemon context was unexpectedly cancelled: %v", daemonCtx.Err())
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("expected ActiveCount == 0, got %d", reg.ActiveCount())
	}

	daemonCancel()
	p.Stop()
}

// TestPool_PendingCancellation_DeliveredOnRegister_AndBlocksExecution verifies:
// 1. An early cancellation intent stored in ExecutionRegistry is delivered when the run registers.
// 2. A run cancelled before scheduler execution does not proceed as a normal execution.
// 3. The scheduler aborts execution and finalizes as StatusCancelled.
// 4. No stale registry entries remain.
func TestPool_PendingCancellation_DeliveredOnRegister_AndBlocksExecution(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_pending_cancel.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-pending-delivery"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-pending",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "rj-pending-1",
		RunID:     runID,
		JobName:   "early-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	reg := NewExecutionRegistry()
	earlyCause := errors.New("early cancellation before dequeue")

	// 1. Record early cancellation intent before worker registers
	dispatched, err := reg.Cancel(runID, earlyCause)
	if err != nil {
		t.Fatalf("early Cancel error: %v", err)
	}
	if dispatched {
		t.Fatal("expected dispatched == false for pending cancellation")
	}
	if !reg.IsPendingCancel(runID) {
		t.Fatal("expected IsPendingCancel == true before registration")
	}

	var runnerExecuted atomic.Bool
	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerExecuted.Store(true)
			return nil
		},
	}

	q := queue.New(10)
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-pending",
			Jobs: []drevtypes.Job{
				{Name: "early-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "echo normal"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Poll until run completes in store
	deadline := time.After(3 * time.Second)
	var finalRun *drevtypes.Run
	for {
		r, err := s.GetRun(ctx, runID)
		if err == nil && r != nil && (r.Status == drevtypes.StatusCancelled || r.Status == drevtypes.StatusFailed) {
			finalRun = r
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for run to complete in store; current status: %v", r)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// 2. Runner must NEVER have executed
	if runnerExecuted.Load() {
		t.Error("runner was executed for a run cancelled prior to execution")
	}

	// 3. Final state must be cancelled
	if finalRun.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", finalRun.Status, drevtypes.StatusCancelled)
	}

	// 4. Registry must be completely clean (no active, no pending)
	if reg.IsActive(runID) {
		t.Error("run remained active in registry")
	}
	if reg.IsPendingCancel(runID) {
		t.Error("run remained pending in registry")
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}
	if reg.PendingCount() != 0 {
		t.Errorf("PendingCount = %d, want 0", reg.PendingCount())
	}

	daemonCancel()
	p.Stop()
}

// TestPool_DaemonShutdown_CancelsActiveRuns verifies:
// 1. Daemon shutdown cancels the daemon context.
// 2. Active run-scoped contexts observe context cancellation.
// 3. Workers clean up and unregister without leaking registry entries.
func TestPool_DaemonShutdown_CancelsActiveRuns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_shutdown.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-shutdown-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-shut",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "rj-shut-1",
		RunID:     runID,
		JobName:   "shut-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	startedCh := make(chan struct{})
	var observedErr error

	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(startedCh)
			<-runCtx.Done()
			observedErr = runCtx.Err()
			return observedErr
		},
	}

	q := queue.New(10)
	reg := NewExecutionRegistry()
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-shut",
			Jobs: []drevtypes.Job{
				{Name: "shut-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "sleep 10"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	select {
	case <-startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runner execution to start")
	}

	if !reg.IsActive(runID) {
		t.Fatal("expected run to be active in registry")
	}

	// Trigger daemon shutdown
	daemonCancel()
	p.Stop()

	if !errors.Is(observedErr, context.Canceled) {
		t.Errorf("expected runner to observe context.Canceled, got %v", observedErr)
	}
	if reg.IsActive(runID) {
		t.Error("expected run to be unregistered after shutdown")
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}
}

// TestPool_ConcurrentRuns_IndependentCancellationContexts verifies:
// 1. Multiple concurrent runs have independent run-scoped cancellation contexts.
// 2. Cancelling run-1 does not cancel run-2.
// 3. Run-2 completes successfully while run-1 terminates due to cancellation.
// 4. Both runs unregister cleanly from registry.
func TestPool_ConcurrentRuns_IndependentCancellationContexts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_concurrent.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	run1ID := "run-conc-1"
	run2ID := "run-conc-2"

	for _, id := range []string{run1ID, run2ID} {
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         id,
			PipelineID: "p-conc",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("creating %s: %v", id, err)
		}
		if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
			ID:        "rj-" + id,
			RunID:     id,
			JobName:   "conc-job",
			Status:    drevtypes.StatusPending,
			StartedAt: time.Now(),
		}); err != nil {
			t.Fatalf("creating rj for %s: %v", id, err)
		}
	}

	var bothStartedWg sync.WaitGroup
	bothStartedWg.Add(2)

	cancelCause1 := errors.New("abort only run 1")
	var run1Cause error
	var run2Completed atomic.Bool

	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			bothStartedWg.Done()
			if run.ID == run1ID {
				<-runCtx.Done()
				run1Cause = context.Cause(runCtx)
				return runCtx.Err()
			}
			// run 2 does brief work and completes successfully
			time.Sleep(50 * time.Millisecond)
			run2Completed.Store(true)
			return nil
		},
	}

	q := queue.New(10)
	reg := NewExecutionRegistry()
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(2, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	for _, id := range []string{run1ID, run2ID} {
		job := &queue.Job{
			RunID: id,
			Pipeline: &drevtypes.Pipeline{
				Name: "pipe-conc",
				Jobs: []drevtypes.Job{
					{Name: "conc-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "echo conc"}}},
				},
			},
			LogWriter:  nopWriteCloser{io.Discard},
			EnqueuedAt: time.Now(),
		}
		if err := q.Enqueue(job); err != nil {
			t.Fatalf("enqueue %s error: %v", id, err)
		}
	}

	bothStartedWg.Wait()

	// Both must be active in registry
	if !reg.IsActive(run1ID) || !reg.IsActive(run2ID) {
		t.Fatalf("expected both runs to be active: run1=%v, run2=%v", reg.IsActive(run1ID), reg.IsActive(run2ID))
	}
	if reg.ActiveCount() != 2 {
		t.Errorf("ActiveCount = %d, want 2", reg.ActiveCount())
	}

	// Cancel only run-1
	dispatched, err := reg.Cancel(run1ID, cancelCause1)
	if err != nil || !dispatched {
		t.Fatalf("Cancel(run1) error=%v, dispatched=%v", err, dispatched)
	}

	// Poll until both finish
	deadline := time.After(3 * time.Second)
	for {
		if !reg.IsActive(run1ID) && !reg.IsActive(run2ID) && reg.ActiveCount() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for concurrent runs to finish; active count=%d", reg.ActiveCount())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if !errors.Is(run1Cause, cancelCause1) {
		t.Errorf("run1 cause = %v, want %v", run1Cause, cancelCause1)
	}
	if !run2Completed.Load() {
		t.Error("run2 was unexpectedly aborted or failed to complete")
	}

	// Verify run-2 succeeded in store
	r2, err := s.GetRun(ctx, run2ID)
	if err != nil || r2 == nil {
		t.Fatalf("loading run2: %v", err)
	}
	if r2.Status != drevtypes.StatusSuccess {
		t.Errorf("run2 status = %s, want %s", r2.Status, drevtypes.StatusSuccess)
	}

	daemonCancel()
	p.Stop()
}

// TestPool_ClaimFailure_DoesNotRegisterOrExecute verifies:
// 1. When ClaimRunForExecution fails (e.g. run is already terminal), the worker does not register.
// 2. The scheduler is never invoked.
// 3. No dangling registry entry exists.
func TestPool_ClaimFailure_DoesNotRegisterOrExecute(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_claim_fail.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-claim-fail"
	// Create run already in terminal 'failed' status
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-claim-fail",
		Status:     drevtypes.StatusFailed,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}

	var runnerExecuted atomic.Bool
	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerExecuted.Store(true)
			return nil
		},
	}

	q := queue.New(10)
	reg := NewExecutionRegistry()
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-claim-fail",
			Jobs: []drevtypes.Job{
				{Name: "job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "echo hi"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Give worker time to process and reject claim
	time.Sleep(100 * time.Millisecond)

	if runnerExecuted.Load() {
		t.Error("runner was executed for an unclaimable run")
	}
	if reg.IsActive(runID) {
		t.Error("unclaimable run was left active in registry")
	}
	if reg.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", reg.ActiveCount())
	}

	daemonCancel()
	p.Stop()
}

// TestPool_UnregistrationOnSchedulerError verifies:
// 1. When the scheduler returns an execution error, the worker still unregisters from registry.
// 2. Active count returns to 0.
func TestPool_UnregistrationOnSchedulerError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_sched_err.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-sched-err"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-sched-err",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "rj-err-1",
		RunID:     runID,
		JobName:   "err-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	mr := &poolMockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return errors.New("simulated fatal build step error")
		},
	}

	q := queue.New(10)
	reg := NewExecutionRegistry()
	sched := scheduler.New(mr, s)
	p := NewWithRegistry(1, q, sched, s, nil, reg)

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	defer daemonCancel()
	p.Start(daemonCtx)

	job := &queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-err",
			Jobs: []drevtypes.Job{
				{Name: "err-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "step", Run: "exit 1"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Poll until worker unregisters
	deadline := time.After(3 * time.Second)
	for {
		if !reg.IsActive(runID) && reg.ActiveCount() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for unregistration on error; active count=%d", reg.ActiveCount())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	daemonCancel()
	p.Stop()
}
