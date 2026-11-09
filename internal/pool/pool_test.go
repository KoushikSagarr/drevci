package pool

import (
	"context"
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

// nopWriteCloser is an io.WriteCloser that discards all writes.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func makeJob(id string) *queue.Job {
	return &queue.Job{
		RunID:      id,
		Pipeline:   &drevtypes.Pipeline{Name: id},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}
}

// testPool is a minimal pool that uses a plain function instead of the real scheduler/store.
type testPool struct {
	workers int
	queue   *queue.Queue
	fn      func(context.Context, *queue.Job)
	wg      sync.WaitGroup
}

func newTestPool(workers int, q *queue.Queue, fn func(context.Context, *queue.Job)) *testPool {
	return &testPool{workers: workers, queue: q, fn: fn}
}

func (p *testPool) start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case job := <-p.queue.Drain():
					p.fn(ctx, job)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (p *testPool) stop() {
	p.wg.Wait()
}

func TestPool_ProcessesJobs(t *testing.T) {
	q := queue.New(10)
	processed := &atomic.Int32{}

	tp := newTestPool(2, q, func(ctx context.Context, job *queue.Job) {
		time.Sleep(50 * time.Millisecond) // simulate work
		processed.Add(1)
		job.LogWriter.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	tp.start(ctx)

	for i := 0; i < 4; i++ {
		q.Enqueue(makeJob(string(rune('a' + i))))
	}

	deadline := time.After(5 * time.Second)
	for {
		if processed.Load() == 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: only %d/4 jobs completed", processed.Load())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	tp.stop()
}

func TestPool_CancelContext(t *testing.T) {
	q := queue.New(10)
	tp := newTestPool(2, q, func(ctx context.Context, job *queue.Job) {
		job.LogWriter.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	tp.start(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		tp.stop()
		close(done)
	}()

	select {
	case <-done:
		// pool stopped cleanly
	case <-time.After(3 * time.Second):
		t.Fatal("pool did not stop cleanly after context cancellation")
	}
}

func TestPool_RealPool_ContextCancelAllowsStop(t *testing.T) {
	q := queue.New(10)
	p := New(3, q, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	// Cancelling the context must allow p.Stop() to return without deadlocking
	cancel()

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean return verified
	case <-time.After(3 * time.Second):
		t.Fatal("real Pool.Stop() deadlocked after context cancellation")
	}
}

type poolMockRunner struct {
	runFunc func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error
}

func (m *poolMockRunner) RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
	if m.runFunc != nil {
		return m.runFunc(ctx, run, job, w, logWriter)
	}
	return nil
}

func TestPool_WorkerAvailableAfterJobTimeout(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_timeout_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	run1 := &drevtypes.Run{
		ID:         "run-timeout-1",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run1); err != nil {
		t.Fatalf("creating run1: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-1",
		RunID:     run1.ID,
		JobName:   "timeout-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj1: %v", err)
	}

	run2 := &drevtypes.Run{
		ID:         "run-quick-2",
		PipelineID: "p2",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run2); err != nil {
		t.Fatalf("creating run2: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-2",
		RunID:     run2.ID,
		JobName:   "quick-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj2: %v", err)
	}

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "timeout-job" {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(10)
	// Single worker pool
	p := New(1, q, sched, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	// Enqueue job 1 (will timeout)
	if err := q.Enqueue(&queue.Job{
		RunID: run1.ID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-1",
			Jobs: []drevtypes.Job{
				{Name: "timeout-job", Image: "alpine", Timeout: "30ms", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 10"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job 1: %v", err)
	}

	// Enqueue job 2 (should be picked up by the same worker after job 1 times out)
	if err := q.Enqueue(&queue.Job{
		RunID: run2.ID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-2",
			Jobs: []drevtypes.Job{
				{Name: "quick-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo ok"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job 2: %v", err)
	}

	// Wait for run2 to finish with StatusSuccess
	deadline := time.After(3 * time.Second)
	success := false
	for !success {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for worker to process run2 after run1 timeout")
		default:
			r2, err := s.GetRun(context.Background(), run2.ID)
			if err == nil && r2.Status == drevtypes.StatusSuccess {
				success = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	cancel()
	p.Stop()

	// Verify run1 is StatusFailed
	r1, err := s.GetRun(context.Background(), run1.ID)
	if err != nil {
		t.Fatalf("getting run1: %v", err)
	}
	if r1.Status != drevtypes.StatusFailed {
		t.Errorf("expected run1 to have status %q, got %q", drevtypes.StatusFailed, r1.Status)
	}

	// Verify run2 is StatusSuccess
	r2, err := s.GetRun(context.Background(), run2.ID)
	if err != nil {
		t.Fatalf("getting run2: %v", err)
	}
	if r2.Status != drevtypes.StatusSuccess {
		t.Errorf("expected run2 to have status %q, got %q", drevtypes.StatusSuccess, r2.Status)
	}
}
