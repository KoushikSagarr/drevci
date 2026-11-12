package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

func TestOutboxRecoverer_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery_success.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	spec := `
name: pipeline-recover
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: step1
        run: echo ok
`

	// Seed 3 pending runs in outbox
	for i := 1; i <= 3; i++ {
		runID := fmt.Sprintf("run-rec-%d", i)
		del := &drevtypes.WebhookDelivery{DeliveryID: fmt.Sprintf("del-%d", i), RunID: runID}
		run := &drevtypes.Run{ID: runID, PipelineID: "p1", Status: drevtypes.StatusPending}
		jobs := []*drevtypes.RunJob{{ID: fmt.Sprintf("j-%d", i), RunID: runID, JobName: "test-job", Status: drevtypes.StatusPending}}
		if err := s.CreateRunWithDelivery(ctx, del, run, jobs, spec); err != nil {
			t.Fatalf("CreateRunWithDelivery(%d): %v", i, err)
		}
	}

	outboxBefore, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxBefore) != 3 {
		t.Fatalf("expected 3 outbox items, got %d", len(outboxBefore))
	}

	q := queue.New(10)
	recoverer := NewOutboxRecoverer(s, q, nil)

	if err := recoverer.Recover(ctx); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if q.Depth() != 3 {
		t.Fatalf("expected 3 jobs in queue, got %d", q.Depth())
	}

	// Now start workers to process the recovered runs
	var processed atomic.Int32
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			processed.Add(1)
			return nil
		},
	}
	sched := scheduler.New(mr, s)
	p := New(2, q, sched, s, nil)

	poolCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(poolCtx)

	// Wait for all 3 to process
	deadline := time.Now().Add(3 * time.Second)
	for processed.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processed.Load() != 3 {
		t.Fatalf("expected 3 jobs processed, got %d", processed.Load())
	}

	outboxAfter, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxAfter) != 0 {
		t.Errorf("expected 0 outbox items after execution, got %d", len(outboxAfter))
	}
}

func TestOutboxRecoverer_QueueFull_NoLoss(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery_full.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	spec := `
name: pipeline-full
jobs:
  - name: job1
    image: alpine
    steps:
      - name: s
        run: echo hi
`

	// Seed 5 pending outbox items
	for i := 1; i <= 5; i++ {
		runID := fmt.Sprintf("run-full-%d", i)
		del := &drevtypes.WebhookDelivery{DeliveryID: fmt.Sprintf("del-%d", i), RunID: runID}
		run := &drevtypes.Run{ID: runID, PipelineID: "p-full", Status: drevtypes.StatusPending}
		jobs := []*drevtypes.RunJob{{ID: fmt.Sprintf("j-%d", i), RunID: runID, JobName: "job1", Status: drevtypes.StatusPending}}
		if err := s.CreateRunWithDelivery(ctx, del, run, jobs, spec); err != nil {
			t.Fatalf("seed failed: %v", err)
		}
	}

	// Queue capacity is deliberately small (1) to force blocking and backpressure
	q := queue.New(1)

	var processed atomic.Int32
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			time.Sleep(10 * time.Millisecond)
			processed.Add(1)
			return nil
		},
	}
	sched := scheduler.New(mr, s)
	p := New(1, q, sched, s, nil)

	poolCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(poolCtx)

	recoverer := NewOutboxRecoverer(s, q, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- recoverer.Recover(poolCtx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("recover returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recover timed out")
	}

	// Wait for all 5 to complete
	deadline := time.Now().Add(3 * time.Second)
	for processed.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processed.Load() != 5 {
		t.Fatalf("expected all 5 runs to be processed despite queue size 1, got %d", processed.Load())
	}

	outboxAfter, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxAfter) != 0 {
		t.Errorf("expected outbox empty, got %d items", len(outboxAfter))
	}
}

func TestOutboxRecoverer_Cancellation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery_cancel.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	spec := `
name: pipeline-cancel
jobs:
  - name: j
    image: alpine
    steps:
      - name: s
        run: echo ok
`
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		runID := fmt.Sprintf("run-c-%d", i)
		_ = s.CreateRunWithDelivery(ctx,
			&drevtypes.WebhookDelivery{DeliveryID: fmt.Sprintf("del-c-%d", i), RunID: runID},
			&drevtypes.Run{ID: runID, PipelineID: "p", Status: drevtypes.StatusPending},
			[]*drevtypes.RunJob{{ID: fmt.Sprintf("jc-%d", i), RunID: runID, JobName: "j", Status: drevtypes.StatusPending}},
			spec,
		)
	}

	// Queue size 1 with no workers consuming -> will block on second item
	q := queue.New(1)
	recoverer := NewOutboxRecoverer(s, q, nil)

	cancelCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = recoverer.Recover(cancelCtx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context deadline/canceled error, got: %v", err)
	}

	// Ensure pending items in DB remain intact
	remaining, err := s.GetPendingOutbox(context.Background(), 10)
	if err != nil || len(remaining) != 3 {
		t.Fatalf("all outbox items must remain intact after recovery cancellation, got: %d", len(remaining))
	}
}

func TestPool_WorkerRefusesUnclaimedRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "refuse_claim.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Seed run that is already in 'running' state (claimed by another worker)
	runID := "run-already-running"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-refuse",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now().UTC(),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	var pipelineExecuted atomic.Bool
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			pipelineExecuted.Store(true)
			return nil
		},
	}
	sched := scheduler.New(mr, s)
	q := queue.New(5)
	p := New(1, q, sched, s, nil)

	poolCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(poolCtx)

	// Enqueue job for already claimed run
	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe",
			Jobs: []drevtypes.Job{{Name: "j1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo 1"}}}},
		},
		LogWriter:  recoveryNopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if pipelineExecuted.Load() {
		t.Fatalf("worker must NOT execute pipeline when durable claim fails")
	}
}

func TestPool_ClaimOccursBeforeSideEffects(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "claim_ordering.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-order-1"
	spec := `
name: pipe-order
jobs:
  - name: j1
    image: alpine
    steps:
      - name: s
        run: echo 1
`
	del := &drevtypes.WebhookDelivery{DeliveryID: "del-order-1", RunID: runID}
	run := &drevtypes.Run{ID: runID, PipelineID: "p-order", Status: drevtypes.StatusPending}
	jobs := []*drevtypes.RunJob{{ID: "job-order-1", RunID: runID, JobName: "j1", Status: drevtypes.StatusPending}}

	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, spec); err != nil {
		t.Fatalf("CreateRunWithDelivery: %v", err)
	}

	var claimVerifiedInRunner atomic.Bool
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, r *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			// At the moment of runner side effect execution, the database state must already be 'running'
			// and outbox entry must already be deleted!
			dbRun, err := s.GetRun(context.Background(), runID)
			if err != nil || dbRun == nil {
				t.Errorf("GetRun failed inside runner: %v", err)
				return err
			}
			if dbRun.Status != drevtypes.StatusRunning {
				t.Errorf("expected run status running inside runner, got: %s", dbRun.Status)
			}
			if dbRun.StartedAt.IsZero() {
				t.Errorf("expected started_at set inside runner, got zero")
			}

			// Outbox entry must be gone
			outbox, err := s.GetPendingOutbox(context.Background(), 10)
			if err != nil || len(outbox) != 0 {
				t.Errorf("expected outbox entry to be deleted before runner executes, got: %d items", len(outbox))
			}

			claimVerifiedInRunner.Store(true)
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(5)
	p := New(1, q, sched, s, nil)

	poolCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(poolCtx)

	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-order",
			Jobs: []drevtypes.Job{{Name: "j1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo 1"}}}},
		},
		LogWriter:  recoveryNopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !claimVerifiedInRunner.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if !claimVerifiedInRunner.Load() {
		t.Fatalf("runner did not execute or claim verification failed")
	}
}

func TestOutboxRecoverer_InvalidPipelineSpec_MarksRunFailedAndDeletesOutbox(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery_invalid_spec.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	runID := "run-invalid-spec-1"
	del := &drevtypes.WebhookDelivery{DeliveryID: "del-inv-1", RunID: runID}
	run := &drevtypes.Run{ID: runID, PipelineID: "p-inv", Status: drevtypes.StatusPending}
	jobs := []*drevtypes.RunJob{{ID: "job-inv-1", RunID: runID, JobName: "j1", Status: drevtypes.StatusPending}}

	// Seed outbox with invalid YAML (not a valid pipeline)
	invalidSpec := "this is : : not valid : yaml : : :"
	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, invalidSpec); err != nil {
		t.Fatalf("CreateRunWithDelivery: %v", err)
	}

	outboxBefore, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxBefore) != 1 {
		t.Fatalf("expected 1 outbox item before recovery, got %d", len(outboxBefore))
	}

	q := queue.New(10)
	recoverer := NewOutboxRecoverer(s, q, nil)

	if err := recoverer.Recover(ctx); err != nil {
		t.Fatalf("recoverer.Recover failed: %v", err)
	}

	// Queue should have 0 items because the invalid spec could not be enqueued
	if q.Depth() != 0 {
		t.Errorf("expected 0 jobs in queue for invalid spec, got %d", q.Depth())
	}

	// Run must be marked StatusFailed in the store
	dbRun, err := s.GetRun(ctx, runID)
	if err != nil || dbRun == nil {
		t.Fatalf("failed to get run from store: %v", err)
	}
	if dbRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected run status StatusFailed, got %s", dbRun.Status)
	}

	// Outbox entry must be cleared so it doesn't block recovery on subsequent passes
	outboxAfter, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxAfter) != 0 {
		t.Errorf("expected outbox to be cleared for invalid spec, got %d items", len(outboxAfter))
	}
}

func TestOutboxRecoverer_PreservesPRMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery_pr_metadata.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	pipeline := &drevtypes.Pipeline{
		Name: "pipeline-pr-recover",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       "https://github.com/octocat/hello-world.git",
			Ref:       "refs/pull/42/head",
			CommitSHA: "b1b2c3d4e5f678901234567890abcdef12345678",
			PRNumber:  42,
		},
		Jobs: []drevtypes.Job{
			{
				Name:  "test-job",
				Image: "alpine",
				Steps: []drevtypes.Step{
					{Name: "step1", Run: "echo ok"},
				},
			},
		},
	}

	specBytes, err := json.Marshal(pipeline)
	if err != nil {
		t.Fatalf("marshaling pipeline spec: %v", err)
	}

	runID := "run-pr-recover-1"
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pr-1",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   42,
		RunID:      runID,
	}
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: pipeline.Name,
		Status:     drevtypes.StatusPending,
		CommitSHA:  pipeline.Source.CommitSHA,
		Branch:     "feature-login",
	}
	jobs := []*drevtypes.RunJob{
		{
			ID:      "job-rec-1",
			RunID:   runID,
			JobName: "test-job",
			Status:  drevtypes.StatusPending,
		},
	}

	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, string(specBytes)); err != nil {
		t.Fatalf("CreateRunWithDelivery: %v", err)
	}

	q := queue.New(10)
	recoverer := NewOutboxRecoverer(s, q, nil)

	if err := recoverer.Recover(ctx); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if q.Depth() != 1 {
		t.Fatalf("expected 1 job in queue, got %d", q.Depth())
	}

	select {
	case job := <-q.Drain():
		if job.RunID != runID {
			t.Errorf("expected RunID %s, got %s", runID, job.RunID)
		}
		if job.Pipeline.Source.URL != pipeline.Source.URL {
			t.Errorf("expected Source.URL %s, got %s", pipeline.Source.URL, job.Pipeline.Source.URL)
		}
		if job.Pipeline.Source.Ref != pipeline.Source.Ref {
			t.Errorf("expected Source.Ref %s, got %s", pipeline.Source.Ref, job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.CommitSHA != pipeline.Source.CommitSHA {
			t.Errorf("expected Source.CommitSHA %s, got %s", pipeline.Source.CommitSHA, job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.PRNumber != pipeline.Source.PRNumber {
			t.Errorf("expected Source.PRNumber %d, got %d", pipeline.Source.PRNumber, job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected job to be readable from queue")
	}
}
