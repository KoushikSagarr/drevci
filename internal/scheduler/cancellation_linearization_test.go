package scheduler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// TestScheduler_EarlyCancellation_NoWorkspace_NoClone_NoRunner verifies:
// 1. An already-cancelled context at RunPipeline entry aborts immediately.
// 2. No workspace directory is created.
// 3. No repository clone is executed.
// 4. No runner is invoked.
// 5. Run status is finalized as StatusCancelled with failure_reason = 'cancelled' and stage = 'preparation'.
func TestScheduler_EarlyCancellation_NoWorkspace_NoClone_NoRunner(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "early-cancel-pipe",
		Source: drevtypes.Source{
			URL: "https://github.com/drevci/repo.git",
			Ref: "main",
		},
		Jobs: []drevtypes.Job{
			{Name: "job1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s1", Run: "echo hi"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	wsDir := filepath.Join(t.TempDir(), "workspaces")
	var runnerCalled atomic.Bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled.Store(true)
			return nil
		},
	}

	sched := New(mr, s, WithWorkspaceDir(wsDir))

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	if err == nil {
		t.Fatal("expected error from already-cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	if runnerCalled.Load() {
		t.Error("runner was invoked on an already-cancelled run")
	}

	// Verify no workspace directory was created for this run
	expectedRunWsDir := filepath.Join(wsDir, runID)
	if _, statErr := os.Stat(expectedRunWsDir); !os.IsNotExist(statErr) {
		t.Errorf("expected workspace dir %s to not exist, statErr=%v", expectedRunWsDir, statErr)
	}

	// Verify run status in store
	persisted, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("loading run: %v", err)
	}
	if persisted.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", persisted.Status, drevtypes.StatusCancelled)
	}
	if persisted.FailureReason != string(drevtypes.StatusCancelled) {
		t.Errorf("failure_reason = %s, want %s", persisted.FailureReason, drevtypes.StatusCancelled)
	}
	if persisted.InterruptedStage != string(drevtypes.StagePreparation) {
		t.Errorf("interrupted_stage = %s, want %s", persisted.InterruptedStage, drevtypes.StagePreparation)
	}
}

// TestScheduler_RootJobs_DoNotStartAfterCancellation verifies:
// Root/ready DAG jobs (zero dependencies) do not start or create job workspaces if context is cancelled.
func TestScheduler_RootJobs_DoNotStartAfterCancellation(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "root-cancel-pipe",
		Jobs: []drevtypes.Job{
			{Name: "root1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo root"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	wsDir := filepath.Join(t.TempDir(), "workspaces")
	ctx, cancel := context.WithCancel(context.Background())

	var runnerCalled atomic.Bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled.Store(true)
			return nil
		},
	}

	sched := New(mr, s, WithWorkspaceDir(wsDir))

	// Cancel context before DAG jobs begin
	cancel()

	err := sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if runnerCalled.Load() {
		t.Error("root job runner was invoked after context was cancelled")
	}

	runJobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("loading run jobs: %v", err)
	}
	if len(runJobs) != 1 {
		t.Fatalf("expected 1 run job, got %d", len(runJobs))
	}
	if runJobs[0].Status != drevtypes.StatusCancelled {
		t.Errorf("job status = %s, want %s", runJobs[0].Status, drevtypes.StatusCancelled)
	}

	// Verify no job workspace was created on disk
	jobWsDir := filepath.Join(wsDir, runID, "jobs", "root1")
	if _, statErr := os.Stat(jobWsDir); !os.IsNotExist(statErr) {
		t.Errorf("expected job workspace %s to not exist, statErr=%v", jobWsDir, statErr)
	}
}

// TestScheduler_DependencyWaitingJobs_BecomeCancelled verifies:
// 1. Dependency-waiting jobs become cancelled when the run context is cancelled.
// 2. Running jobs observe context cancellation and terminate.
// 3. The run finalizes as StatusCancelled.
func TestScheduler_DependencyWaitingJobs_BecomeCancelled(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "dep-cancel-pipe",
		Jobs: []drevtypes.Job{
			{Name: "jobA", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo a"}}},
			{Name: "jobB", DependsOn: []string{"jobA"}, Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo b"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	jobAStarted := make(chan struct{})
	var jobBStarted atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "jobA" {
				close(jobAStarted)
				<-runCtx.Done()
				return runCtx.Err()
			}
			if job.Name == "jobB" {
				jobBStarted.Store(true)
			}
			return nil
		},
	}

	sched := New(mr, s)

	done := make(chan error, 1)
	go func() {
		done <- sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	}()

	select {
	case <-jobAStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for jobA to start")
	}

	// Cancel context while jobA is running and jobB is waiting on jobA
	cancel()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunPipeline to exit after cancellation")
	}

	if jobBStarted.Load() {
		t.Error("jobB was executed despite jobA and run being cancelled")
	}

	// Verify job statuses in store
	runJobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}

	jobStatusMap := make(map[string]drevtypes.RunStatus)
	for _, rj := range runJobs {
		jobStatusMap[rj.JobName] = rj.Status
	}

	if jobStatusMap["jobA"] != drevtypes.StatusCancelled {
		t.Errorf("jobA status = %s, want %s", jobStatusMap["jobA"], drevtypes.StatusCancelled)
	}
	if jobStatusMap["jobB"] != drevtypes.StatusCancelled {
		t.Errorf("jobB status = %s, want %s", jobStatusMap["jobB"], drevtypes.StatusCancelled)
	}

	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %s, want %s", run.Status, drevtypes.StatusCancelled)
	}
	if run.FailureReason != string(drevtypes.StatusCancelled) {
		t.Errorf("run failure_reason = %s, want %s", run.FailureReason, drevtypes.StatusCancelled)
	}
}

// TestScheduler_PrecedingFailurePrecedence_Preserved verifies:
// If a job genuinely failed before cancellation took effect:
// 1. The run finalizes as StatusFailed with failure_reason = 'job_failed'.
// 2. Unstarted jobs become cancelled.
// 3. Genuine failure precedence is strictly preserved.
func TestScheduler_PrecedingFailurePrecedence_Preserved(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "preceding-fail-pipe",
		Jobs: []drevtypes.Job{
			{Name: "job-fail", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "fail"}}},
			{Name: "job-cancel", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "wait"}}},
			{Name: "job-dep", DependsOn: []string{"job-fail"}, Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "skip"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failJobDone := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "job-fail" {
				defer close(failJobDone)
				// Genuine build failure
				return errors.New("command failed with exit code 1")
			}
			if job.Name == "job-cancel" {
				// Wait until job-fail fails, then cancel context
				<-failJobDone
				cancel()
				<-runCtx.Done()
				return runCtx.Err()
			}
			return nil
		},
	}

	sched := New(mr, s)
	err := sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	if err == nil {
		t.Fatal("expected RunPipeline to return error for genuine failure, got nil")
	}

	// Must finalize as StatusFailed due to preceding failure precedence
	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusFailed {
		t.Errorf("run status = %s, want %s (Preceding Failure Precedence violated)", run.Status, drevtypes.StatusFailed)
	}
	if run.FailureReason != drevtypes.FailureReasonJobFailed {
		t.Errorf("failure_reason = %s, want %s", run.FailureReason, drevtypes.FailureReasonJobFailed)
	}

	runJobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	statusMap := make(map[string]drevtypes.RunStatus)
	for _, rj := range runJobs {
		statusMap[rj.JobName] = rj.Status
	}

	if statusMap["job-fail"] != drevtypes.StatusFailed {
		t.Errorf("job-fail status = %s, want %s", statusMap["job-fail"], drevtypes.StatusFailed)
	}
	if statusMap["job-cancel"] != drevtypes.StatusCancelled {
		t.Errorf("job-cancel status = %s, want %s", statusMap["job-cancel"], drevtypes.StatusCancelled)
	}
}

// TestScheduler_CancellationWithoutPrecedingFailure_FinalizesCancelled verifies:
// If zero jobs fail and cancellation occurs, the run finalizes as StatusCancelled.
func TestScheduler_CancellationWithoutPrecedingFailure_FinalizesCancelled(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "clean-cancel-pipe",
		Jobs: []drevtypes.Job{
			{Name: "j1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 10"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(started)
			<-runCtx.Done()
			return runCtx.Err()
		},
	}

	sched := New(mr, s)
	done := make(chan error, 1)
	go func() {
		done <- sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	}()

	<-started
	cancel()

	err := <-done
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %s, want %s", run.Status, drevtypes.StatusCancelled)
	}
	if run.FailureReason != string(drevtypes.StatusCancelled) {
		t.Errorf("failure_reason = %s, want %s", run.FailureReason, drevtypes.StatusCancelled)
	}
	if run.InterruptedStage != string(drevtypes.StageExecution) {
		t.Errorf("interrupted_stage = %s, want %s", run.InterruptedStage, drevtypes.StageExecution)
	}
}

// TestScheduler_DatabaseWinnerSemantics_CancellationIntentBlocksSuccess verifies:
// 1. All DAG jobs complete with success.
// 2. Cancellation intent was persisted in the database before scheduler finalization.
// 3. Conditional update to success is rejected with ErrCancellationRequested.
// 4. Scheduler detects ErrCancellationRequested and finalizes the run as StatusCancelled.
// 5. RunPipeline returns context.Canceled.
func TestScheduler_DatabaseWinnerSemantics_CancellationIntentBlocksSuccess(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "winner-cancel-pipe",
		Jobs: []drevtypes.Job{
			{Name: "success-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo done"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	// In the runner, simulate cancellation intent committed in the database before finalization
	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			// Persist cancellation intent in the database while the runner is finishing
			cancelRes, err := s.CancelRun(context.Background(), runID, "")
			if err != nil {
				t.Fatalf("CancelRun: %v", err)
			}
			if !cancelRes.WasRunning {
				t.Fatalf("expected WasRunning == true, got %+v", cancelRes)
			}
			return nil // job itself succeeds
		},
	}

	sched := New(mr, s)

	// Run pipeline with uncancelled context to ensure cancellation is detected via database
	ctx := context.Background()
	err := sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	if err == nil {
		t.Fatal("expected RunPipeline to reject success when cancellation intent exists in DB, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// Verify database record: MUST be StatusCancelled
	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s (Winner semantics violated: success overwrote cancellation intent)", run.Status, drevtypes.StatusCancelled)
	}
	if run.FailureReason != string(drevtypes.StatusCancelled) {
		t.Errorf("failure_reason = %s, want %s", run.FailureReason, drevtypes.StatusCancelled)
	}
}

// TestScheduler_DatabaseWinnerSemantics_SuccessWinsFirst verifies:
// 1. All DAG jobs succeed and scheduler successfully commits StatusSuccess.
// 2. A subsequent CancelRun observes terminal status and returns ErrRunAlreadyTerminal.
func TestScheduler_DatabaseWinnerSemantics_SuccessWinsFirst(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "winner-success-pipe",
		Jobs: []drevtypes.Job{
			{Name: "quick-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo done"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := New(mr, s)
	if err := sched.RunPipeline(context.Background(), pipeline, runID, io.Discard); err != nil {
		t.Fatalf("RunPipeline error: %v", err)
	}

	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusSuccess {
		t.Fatalf("status = %s, want %s", run.Status, drevtypes.StatusSuccess)
	}

	// Subsequent cancellation attempt must fail with ErrRunAlreadyTerminal
	_, err = s.CancelRun(context.Background(), runID, "")
	if err == nil || !errors.Is(err, store.ErrRunAlreadyTerminal) {
		t.Errorf("expected ErrRunAlreadyTerminal, got: %v", err)
	}
}

// TestScheduler_StoreErrorsDuringFinalization_Surfaced verifies:
// Store errors during success finalization are properly returned and not swallowed.
func TestScheduler_StoreErrorsDuringFinalization_Surfaced(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "store-err-pipe",
		Jobs: []drevtypes.Job{
			{Name: "job1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo done"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	simulatedErr := errors.New("simulated fatal disk I/O error during finalization")
	wrapped := &failFinalizeStoreWrapper{
		Store: s,
		failStatus: func(status drevtypes.RunStatus) error {
			if status == drevtypes.StatusSuccess {
				return simulatedErr
			}
			return nil
		},
	}

	sched := New(mr, wrapped)
	err := sched.RunPipeline(context.Background(), pipeline, runID, io.Discard)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, simulatedErr) {
		t.Errorf("expected error to wrap simulatedErr, got: %v", err)
	}
}

// TestScheduler_EarlyCancellation_StopsBeforeConfigResolution verifies:
// When cancellation occurs right after clone / before config resolution:
// 1. Config resolution is never executed.
// 2. The run finalizes as StatusCancelled with stage 'preparation'.
// 3. Runner is not invoked.
func TestScheduler_EarlyCancellation_StopsBeforeConfigResolution(t *testing.T) {
	repoDir, _ := createTestGitRepo(t, map[string]string{
		".drev.yml": "name: repo-pipe\njobs:\n  - name: j1\n    image: alpine\n    steps:\n      - name: s\n        run: echo hi\n",
	})

	pipeline := &drevtypes.Pipeline{
		Source: drevtypes.Source{
			URL: repoDir,
			Ref: "main",
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())

	var runnerCalled atomic.Bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled.Store(true)
			return nil
		},
	}

	sched := New(mr, s)

	// Custom writer that cancels context as soon as checkout completes in clone
	tw := &triggerWriter{
		trigger: "successfully checked out",
		onMatch: cancel,
	}

	err := sched.RunPipeline(ctx, pipeline, runID, tw)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if runnerCalled.Load() {
		t.Error("runner was invoked despite cancellation during preparation")
	}

	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", run.Status, drevtypes.StatusCancelled)
	}
	if run.InterruptedStage != string(drevtypes.StagePreparation) {
		t.Errorf("stage = %s, want %s", run.InterruptedStage, drevtypes.StagePreparation)
	}
}

// TestScheduler_CompletedSuccessfulJobs_RemainSuccess_WhenSiblingCancels verifies:
// 1. Completed successful jobs retain StatusSuccess when sibling jobs are cancelled.
// 2. Active sibling job becomes StatusCancelled.
// 3. Run status finalizes as StatusCancelled (since zero jobs failed).
func TestScheduler_CompletedSuccessfulJobs_RemainSuccess_WhenSiblingCancels(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "success-sibling-cancel-pipe",
		Jobs: []drevtypes.Job{
			{Name: "quick-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "echo quick"}}},
			{Name: "slow-job", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 10"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quickJobDone := make(chan struct{})
	slowJobStarted := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "quick-job" {
				defer close(quickJobDone)
				return nil // succeeds immediately
			}
			if job.Name == "slow-job" {
				// Wait until quick-job has completed
				<-quickJobDone
				close(slowJobStarted)
				<-runCtx.Done()
				return runCtx.Err()
			}
			return nil
		},
	}

	sched := New(mr, s)
	done := make(chan error, 1)
	go func() {
		done <- sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	}()

	select {
	case <-slowJobStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for slow-job to start")
	}

	cancel()

	err := <-done
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	runJobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	statusMap := make(map[string]drevtypes.RunStatus)
	for _, rj := range runJobs {
		statusMap[rj.JobName] = rj.Status
	}

	if statusMap["quick-job"] != drevtypes.StatusSuccess {
		t.Errorf("quick-job status = %s, want %s (Requirement 5 violated)", statusMap["quick-job"], drevtypes.StatusSuccess)
	}
	if statusMap["slow-job"] != drevtypes.StatusCancelled {
		t.Errorf("slow-job status = %s, want %s", statusMap["slow-job"], drevtypes.StatusCancelled)
	}

	run, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %s, want %s", run.Status, drevtypes.StatusCancelled)
	}
}

// TestScheduler_NoZombieJobs_AllReachTerminalState verifies:
// In a multi-job DAG with running, dependency-waiting, and unstarted jobs,
// context cancellation causes every job to reach a terminal state (success, failed, cancelled, or skipped).
// No job is left in pending or running state.
func TestScheduler_NoZombieJobs_AllReachTerminalState(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "no-zombie-pipe",
		Jobs: []drevtypes.Job{
			{Name: "rootA", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "quick"}}},
			{Name: "rootB", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "slow"}}},
			{Name: "midA", DependsOn: []string{"rootA"}, Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "midA"}}},
			{Name: "midB", DependsOn: []string{"rootB"}, Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "midB"}}},
			{Name: "leaf", DependsOn: []string{"midA", "midB"}, Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "leaf"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootADone := make(chan struct{})
	midAStarted := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			switch job.Name {
			case "rootA":
				defer close(rootADone)
				return nil // succeeds
			case "rootB":
				<-runCtx.Done()
				return runCtx.Err()
			case "midA":
				close(midAStarted)
				<-runCtx.Done()
				return runCtx.Err()
			default:
				return nil
			}
		},
	}

	sched := New(mr, s)
	done := make(chan error, 1)
	go func() {
		done <- sched.RunPipeline(ctx, pipeline, runID, io.Discard)
	}()

	select {
	case <-midAStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for midA to start")
	}

	cancel()

	err := <-done
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	runJobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(runJobs) != 5 {
		t.Fatalf("expected 5 run jobs, got %d", len(runJobs))
	}

	for _, rj := range runJobs {
		if !isTerminalRunStatus(rj.Status) {
			t.Errorf("job %s has non-terminal status %s (ZOMBIE JOB DETECTED)", rj.JobName, rj.Status)
		}
	}

	statusMap := make(map[string]drevtypes.RunStatus)
	for _, rj := range runJobs {
		statusMap[rj.JobName] = rj.Status
	}

	if statusMap["rootA"] != drevtypes.StatusSuccess {
		t.Errorf("rootA status = %s, want %s", statusMap["rootA"], drevtypes.StatusSuccess)
	}
	if statusMap["rootB"] != drevtypes.StatusCancelled {
		t.Errorf("rootB status = %s, want %s", statusMap["rootB"], drevtypes.StatusCancelled)
	}
	if statusMap["midA"] != drevtypes.StatusCancelled {
		t.Errorf("midA status = %s, want %s", statusMap["midA"], drevtypes.StatusCancelled)
	}
	if statusMap["midB"] != drevtypes.StatusCancelled {
		t.Errorf("midB status = %s, want %s", statusMap["midB"], drevtypes.StatusCancelled)
	}
	if statusMap["leaf"] != drevtypes.StatusCancelled {
		t.Errorf("leaf status = %s, want %s", statusMap["leaf"], drevtypes.StatusCancelled)
	}
}

// TestScheduler_StoreErrorsDuringCancelledFinalization_SurfacedAndLogged verifies:
// Store errors when updating run status to StatusCancelled are properly logged to logWriter and returned.
func TestScheduler_StoreErrorsDuringCancelledFinalization_SurfacedAndLogged(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "store-cancel-err-pipe",
		Jobs: []drevtypes.Job{
			{Name: "j1", Image: "alpine", Steps: []drevtypes.Step{{Name: "s", Run: "sleep 5"}}},
		},
	}
	s, runID := setupTestPipeline(t, pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(runCtx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(started)
			<-runCtx.Done()
			return runCtx.Err()
		},
	}

	simulatedErr := errors.New("simulated store failure during cancelled finalization")
	wrapped := &failFinalizeStoreWrapper{
		Store: s,
		failStatusDiag: func(status drevtypes.RunStatus) error {
			if status == drevtypes.StatusCancelled {
				return simulatedErr
			}
			return nil
		},
	}

	sched := New(mr, wrapped)
	var logBuf bytes.Buffer

	done := make(chan error, 1)
	go func() {
		done <- sched.RunPipeline(ctx, pipeline, runID, &logBuf)
	}()

	<-started
	cancel()

	err := <-done
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, simulatedErr) {
		t.Errorf("expected error to wrap simulatedErr, got: %v", err)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "failed to update run status to cancelled") {
		t.Errorf("expected log output to mention finalization failure, got: %s", logOutput)
	}
}

type failFinalizeStoreWrapper struct {
	store.Store
	failStatus     func(status drevtypes.RunStatus) error
	failStatusDiag func(status drevtypes.RunStatus) error
}

func (w *failFinalizeStoreWrapper) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	if w.failStatus != nil {
		if err := w.failStatus(status); err != nil {
			return err
		}
	}
	return w.Store.UpdateRunStatus(ctx, id, status)
}

func (w *failFinalizeStoreWrapper) UpdateRunStatusWithDiagnostics(ctx context.Context, id string, status drevtypes.RunStatus, failureReason string, interruptedStage string) error {
	if w.failStatusDiag != nil {
		if err := w.failStatusDiag(status); err != nil {
			return err
		}
	}
	return w.Store.UpdateRunStatusWithDiagnostics(ctx, id, status, failureReason, interruptedStage)
}

type triggerWriter struct {
	trigger string
	onMatch func()
	buf     bytes.Buffer
	mu      sync.Mutex
}

func (w *triggerWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err = w.buf.Write(p)
	if w.onMatch != nil && strings.Contains(string(p), w.trigger) {
		w.onMatch()
		w.onMatch = nil
	}
	return n, err
}
