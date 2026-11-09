package pool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/internal/notify"
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

func TestPool_Cancellation_PreservesStatusCancelledAndNotifies(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_cancel_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-cancel-100"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-cancel",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-cancel-100",
		RunID:     runID,
		JobName:   "cancel-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj: %v", err)
	}

	var notifyMu sync.Mutex
	var receivedSlack map[string]interface{}
	notifyDone := make(chan struct{}, 1)

	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&p)
		notifyMu.Lock()
		receivedSlack = p
		notifyMu.Unlock()
		select {
		case notifyDone <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	jobStarted := make(chan struct{})
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(jobStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(10)
	p := New(1, q, sched, s, notif)

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-cancel",
			Jobs: []drevtypes.Job{
				{Name: "cancel-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 10"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	// Wait until the runner starts executing
	select {
	case <-jobStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}

	// Cancel the context while execution is running
	cancel()
	p.Stop()

	// Verify database record has StatusCancelled and was not overwritten with StatusFailed
	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Fatalf("expected run status %q, got %q (worker pool must not overwrite StatusCancelled with StatusFailed)",
			drevtypes.StatusCancelled, storedRun.Status)
	}
	if storedRun.FinishedAt.IsZero() {
		t.Errorf("expected finished_at to be populated for cancelled run")
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) == 0 || jobs[0].Status != drevtypes.StatusCancelled {
		t.Errorf("expected job status %q, got %v", drevtypes.StatusCancelled, jobs)
	}

	// Verify notification delivery reflects StatusCancelled
	select {
	case <-notifyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for notification delivery")
	}

	notifyMu.Lock()
	payload := receivedSlack
	notifyMu.Unlock()

	attachments, ok := payload["attachments"].([]interface{})
	if !ok || len(attachments) == 0 {
		t.Fatalf("expected attachments in slack notification, got: %v", payload)
	}
	firstAtt := attachments[0].(map[string]interface{})
	blocks, _ := firstAtt["blocks"].([]interface{})
	if len(blocks) == 0 {
		t.Fatalf("expected blocks in attachment")
	}
	textMap, _ := blocks[0].(map[string]interface{})["text"].(map[string]interface{})
	text, _ := textMap["text"].(string)
	if text != "*Pipeline: pipe-cancel*\nStatus: :warning: *Cancelled*" {
		t.Errorf("expected cancelled status text in slack notification, got: %s", text)
	}
}

func TestPool_GenuineFailure_PreservesStatusFailedAndNotifies(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_fail_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-fail-100"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-fail",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-fail-100",
		RunID:     runID,
		JobName:   "fail-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj: %v", err)
	}

	var notifyMu sync.Mutex
	var receivedSlack map[string]interface{}
	notifyDone := make(chan struct{}, 1)

	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&p)
		notifyMu.Lock()
		receivedSlack = p
		notifyMu.Unlock()
		select {
		case notifyDone <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return errors.New("command exited with non-zero code")
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(10)
	p := New(1, q, sched, s, notif)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-fail",
			Jobs: []drevtypes.Job{
				{Name: "fail-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "exit 1"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	select {
	case <-notifyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for notification delivery")
	}

	cancel()
	p.Stop()

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected run status %q, got %q", drevtypes.StatusFailed, storedRun.Status)
	}

	notifyMu.Lock()
	payload := receivedSlack
	notifyMu.Unlock()

	attachments, ok := payload["attachments"].([]interface{})
	if !ok || len(attachments) == 0 {
		t.Fatalf("expected attachments in slack notification, got: %v", payload)
	}
	firstAtt := attachments[0].(map[string]interface{})
	blocks, _ := firstAtt["blocks"].([]interface{})
	textMap, _ := blocks[0].(map[string]interface{})["text"].(map[string]interface{})
	text, _ := textMap["text"].(string)
	if text != "*Pipeline: pipe-fail*\nStatus: :x: *Failed*" {
		t.Errorf("expected failed status text in slack notification, got: %s", text)
	}
}

func TestPool_Success_PreservesStatusSuccessAndNotifies(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_succ_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-succ-100"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-succ",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-succ-100",
		RunID:     runID,
		JobName:   "succ-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj: %v", err)
	}

	var notifyMu sync.Mutex
	var receivedSlack map[string]interface{}
	notifyDone := make(chan struct{}, 1)

	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&p)
		notifyMu.Lock()
		receivedSlack = p
		notifyMu.Unlock()
		select {
		case notifyDone <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(10)
	p := New(1, q, sched, s, notif)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-succ",
			Jobs: []drevtypes.Job{
				{Name: "succ-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo ok"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	select {
	case <-notifyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for notification delivery")
	}

	cancel()
	p.Stop()

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected run status %q, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}

	notifyMu.Lock()
	payload := receivedSlack
	notifyMu.Unlock()

	attachments, ok := payload["attachments"].([]interface{})
	if !ok || len(attachments) == 0 {
		t.Fatalf("expected attachments in slack notification, got: %v", payload)
	}
	firstAtt := attachments[0].(map[string]interface{})
	blocks, _ := firstAtt["blocks"].([]interface{})
	textMap, _ := blocks[0].(map[string]interface{})["text"].(map[string]interface{})
	text, _ := textMap["text"].(string)
	if text != "*Pipeline: pipe-succ*\nStatus: :white_check_mark: *Succeeded*" {
		t.Errorf("expected succeeded status text in slack notification, got: %s", text)
	}
}

func TestPool_SiblingCancellationRacingWithFailure_PreservesStatusFailed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_sibling_fail_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-sibling-fail-100"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-sibling-fail",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-fail-200",
		RunID:     runID,
		JobName:   "failing-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj fail: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-long-200",
		RunID:     runID,
		JobName:   "long-job",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating rj long: %v", err)
	}

	longStarted := make(chan struct{})
	failDone := make(chan struct{})
	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "failing-job" {
				<-longStarted
				defer close(failDone)
				return errors.New("failing job failed")
			}
			close(longStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := scheduler.New(mr, s)
	q := queue.New(10)
	p := New(1, q, sched, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-sibling-fail",
			Jobs: []drevtypes.Job{
				{Name: "failing-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "exit 1"}}},
				{Name: "long-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 10"}}},
			},
		},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	// Wait for failing-job to fail while long-job is running
	select {
	case <-failDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for failing job to fail")
	}

	// Cancel the context while long-job is still executing
	cancel()
	p.Stop()

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	// Because a genuine job failure occurred, the overall run must be StatusFailed, NOT StatusCancelled
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected run status %q, got %q (genuine failure must take precedence over cancellation)",
			drevtypes.StatusFailed, storedRun.Status)
	}
}

type stubStore struct {
	store.Store
	getRunFunc func(ctx context.Context, id string) (*drevtypes.Run, error)
}

func (s *stubStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	if s.getRunFunc != nil {
		return s.getRunFunc(ctx, id)
	}
	return s.Store.GetRun(ctx, id)
}

type syncWriteCloser struct {
	io.Writer
	closed chan struct{}
}

func newSyncWriteCloser() *syncWriteCloser {
	return &syncWriteCloser{
		Writer: io.Discard,
		closed: make(chan struct{}),
	}
}

func (w *syncWriteCloser) Close() error {
	select {
	case <-w.closed:
	default:
		close(w.closed)
	}
	return nil
}

func TestPool_GetRun_DatabaseError_NoNotificationAndNoFabricatedStatus(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_dberr_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-dberr-1"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-dberr",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-dberr-1",
		RunID:     runID,
		JobName:   "job-1",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	var notifyCalls atomic.Int32
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notifyCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	stub := &stubStore{
		Store: s,
		getRunFunc: func(ctx context.Context, id string) (*drevtypes.Run, error) {
			return nil, errors.New("simulated database read failure")
		},
	}

	q := queue.New(10)
	p := New(1, q, sched, stub, notif)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	logCloser := newSyncWriteCloser()
	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-dberr",
			Jobs: []drevtypes.Job{
				{Name: "job-1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo ok"}}},
			},
		},
		LogWriter:  logCloser,
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	// Wait for processJob to finish
	select {
	case <-logCloser.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to complete")
	}

	cancel()
	p.Stop()

	// Verify no notification was sent because GetRun failed
	if calls := notifyCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 notifications on GetRun failure, got %d", calls)
	}

	// Verify real store contains the scheduler's status and was not mutated or fabricated by the pool
	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run from real store: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected store status %q from scheduler, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}
}

func TestPool_GetRun_ReturnsNil_NoNotificationOrMutation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_nilrun_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-nilrun-1"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-nilrun",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-nilrun-1",
		RunID:     runID,
		JobName:   "job-1",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	var notifyCalls atomic.Int32
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notifyCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	stub := &stubStore{
		Store: s,
		getRunFunc: func(ctx context.Context, id string) (*drevtypes.Run, error) {
			return nil, nil // run not found
		},
	}

	q := queue.New(10)
	p := New(1, q, sched, stub, notif)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	logCloser := newSyncWriteCloser()
	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-nilrun",
			Jobs: []drevtypes.Job{
				{Name: "job-1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo ok"}}},
			},
		},
		LogWriter:  logCloser,
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	select {
	case <-logCloser.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to complete")
	}

	cancel()
	p.Stop()

	// Verify no notification was sent because run was nil
	if calls := notifyCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 notifications on nil run, got %d", calls)
	}

	// Verify real store contains scheduler's status unmodified
	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run from real store: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected store status %q from scheduler, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}
}

func TestPool_StoredRun_NonTerminal_WarningAndNoNotification(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool_nonterm_test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer s.Close()

	runID := "run-nonterm-1"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-nonterm",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("creating run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:        "rj-nonterm-1",
		RunID:     runID,
		JobName:   "job-1",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("creating run job: %v", err)
	}

	var notifyCalls atomic.Int32
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notifyCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	notif := notify.New(notify.NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	mr := &poolMockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := scheduler.New(mr, s)
	stub := &stubStore{
		Store: s,
		getRunFunc: func(ctx context.Context, id string) (*drevtypes.Run, error) {
			run, err := s.GetRun(ctx, id)
			if err != nil {
				return nil, err
			}
			// Simulate scheduler persistence failure that left the run in StatusRunning
			run.Status = drevtypes.StatusRunning
			return run, nil
		},
	}

	q := queue.New(10)
	p := New(1, q, sched, stub, notif)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	logCloser := newSyncWriteCloser()
	if err := q.Enqueue(&queue.Job{
		RunID: runID,
		Pipeline: &drevtypes.Pipeline{
			Name: "pipe-nonterm",
			Jobs: []drevtypes.Job{
				{Name: "job-1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo ok"}}},
			},
		},
		LogWriter:  logCloser,
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("enqueuing job: %v", err)
	}

	select {
	case <-logCloser.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to complete")
	}

	cancel()
	p.Stop()

	// Verify no notification was sent for non-terminal run
	if calls := notifyCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 notifications on non-terminal run, got %d", calls)
	}
}
