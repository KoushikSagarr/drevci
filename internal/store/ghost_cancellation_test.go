package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func newGhostTestStore(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "ghost_cancellation_test.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(%s): %v", dbPath, err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s, dbPath
}

// 1. Cancellation recovery — mixed job states
func TestResetGhostRuns_Cancellation_MixedJobStates(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	runID := "run-cancel-mixed-" + uuid.New().String()
	startedAt := time.Now().UTC().Add(-15 * time.Minute)
	cancelTime := time.Now().UTC().Add(-5 * time.Minute)

	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipeline-mixed",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               startedAt,
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	finTime := time.Now().UTC().Add(-8 * time.Minute)
	jobs := []*drevtypes.RunJob{
		{
			ID:         "job-succ-" + uuid.New().String(),
			RunID:      runID,
			JobName:    "job-success",
			Status:     drevtypes.StatusSuccess,
			StartedAt:  startedAt,
			FinishedAt: finTime,
		},
		{
			ID:            "job-fail-" + uuid.New().String(),
			RunID:         runID,
			JobName:       "job-failed",
			Status:        drevtypes.StatusFailed,
			StartedAt:     startedAt,
			FinishedAt:    finTime,
			FailureReason: drevtypes.FailureReasonCommandFailed,
		},
		{
			ID:         "job-skip-" + uuid.New().String(),
			RunID:      runID,
			JobName:    "job-skipped",
			Status:     drevtypes.StatusSkipped,
			StartedAt:  startedAt,
			FinishedAt: finTime,
		},
		{
			ID:        "job-run-" + uuid.New().String(),
			RunID:     runID,
			JobName:   "job-running",
			Status:    drevtypes.StatusRunning,
			StartedAt: startedAt,
		},
		{
			ID:      "job-pend-" + uuid.New().String(),
			RunID:   runID,
			JobName: "job-pending",
			Status:  drevtypes.StatusPending,
		},
	}
	for _, j := range jobs {
		if err := s.CreateRunJob(ctx, j); err != nil {
			t.Fatalf("CreateRunJob(%s): %v", j.ID, err)
		}
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns failed: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updatedRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %s, want %s", updatedRun.Status, drevtypes.StatusCancelled)
	}
	if updatedRun.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("run failure_reason = %q, want %q", updatedRun.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if updatedRun.InterruptedStage != drevtypes.StageExecution {
		t.Errorf("run interrupted_stage = %q, want %q", updatedRun.InterruptedStage, drevtypes.StageExecution)
	}
	if updatedRun.FinishedAt.IsZero() {
		t.Errorf("run finished_at was not populated")
	}
	if updatedRun.CancellationRequestedAt == nil || !updatedRun.CancellationRequestedAt.Equal(cancelTime) {
		t.Errorf("run cancellation_requested_at corrupted: got %v, want %v", updatedRun.CancellationRequestedAt, cancelTime)
	}

	updatedJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range updatedJobs {
		jobMap[j.JobName] = j
	}

	// Active running job transitioned to cancelled
	jRun := jobMap["job-running"]
	if jRun.Status != drevtypes.StatusCancelled {
		t.Errorf("job-running status = %s, want %s", jRun.Status, drevtypes.StatusCancelled)
	}
	if jRun.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("job-running failure_reason = %q, want %q", jRun.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if jRun.FinishedAt.IsZero() {
		t.Errorf("job-running finished_at was not populated")
	}

	// Unstarted pending job transitioned to cancelled
	jPend := jobMap["job-pending"]
	if jPend.Status != drevtypes.StatusCancelled {
		t.Errorf("job-pending status = %s, want %s", jPend.Status, drevtypes.StatusCancelled)
	}
	if jPend.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("job-pending failure_reason = %q, want %q", jPend.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if jPend.FinishedAt.IsZero() {
		t.Errorf("job-pending finished_at was not populated")
	}

	// Terminal jobs strictly preserved
	jSucc := jobMap["job-success"]
	if jSucc.Status != drevtypes.StatusSuccess {
		t.Errorf("job-success status = %s, want %s", jSucc.Status, drevtypes.StatusSuccess)
	}
	if jSucc.FailureReason != "" {
		t.Errorf("job-success failure_reason = %q, want empty", jSucc.FailureReason)
	}

	jFail := jobMap["job-failed"]
	if jFail.Status != drevtypes.StatusFailed {
		t.Errorf("job-failed status = %s, want %s", jFail.Status, drevtypes.StatusFailed)
	}
	if jFail.FailureReason != drevtypes.FailureReasonCommandFailed {
		t.Errorf("job-failed failure_reason = %q, want %q", jFail.FailureReason, drevtypes.FailureReasonCommandFailed)
	}

	jSkip := jobMap["job-skipped"]
	if jSkip.Status != drevtypes.StatusSkipped {
		t.Errorf("job-skipped status = %s, want %s", jSkip.Status, drevtypes.StatusSkipped)
	}
	if jSkip.FailureReason != "" {
		t.Errorf("job-skipped failure_reason = %q, want empty", jSkip.FailureReason)
	}
}

// 2. Cancellation recovery — preparation stage
func TestResetGhostRuns_Cancellation_PreparationStage(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	cancelTime := time.Now().UTC().Add(-2 * time.Minute)
	runID := "run-prep-cancel-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipeline-prep",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               time.Now().UTC().Add(-10 * time.Minute),
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updated, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updated.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", updated.Status, drevtypes.StatusCancelled)
	}
	if updated.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("failure_reason = %q, want %q", updated.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if updated.InterruptedStage != drevtypes.StagePreparation {
		t.Errorf("interrupted_stage = %q, want %q", updated.InterruptedStage, drevtypes.StagePreparation)
	}
}

// 3. Cancellation recovery — execution stage
func TestResetGhostRuns_Cancellation_ExecutionStage(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	cancelTime := time.Now().UTC().Add(-2 * time.Minute)
	runID := "run-exec-cancel-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipeline-exec",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               time.Now().UTC().Add(-10 * time.Minute),
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "job-exec-" + uuid.New().String(),
		RunID:     runID,
		JobName:   "worker-step",
		Status:    drevtypes.StatusRunning,
		StartedAt: time.Now().UTC().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updated, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updated.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", updated.Status, drevtypes.StatusCancelled)
	}
	if updated.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("failure_reason = %q, want %q", updated.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if updated.InterruptedStage != drevtypes.StageExecution {
		t.Errorf("interrupted_stage = %q, want %q", updated.InterruptedStage, drevtypes.StageExecution)
	}
}

// 4. Cancellation recovery — finalization stage
func TestResetGhostRuns_Cancellation_FinalizationStage(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	cancelTime := time.Now().UTC().Add(-1 * time.Minute)
	runID := "run-fin-cancel-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipeline-fin",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               time.Now().UTC().Add(-10 * time.Minute),
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:         "job-fin-1-" + uuid.New().String(),
		RunID:      runID,
		JobName:    "step-1",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  time.Now().UTC().Add(-9 * time.Minute),
		FinishedAt: time.Now().UTC().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateRunJob 1: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:            "job-fin-2-" + uuid.New().String(),
		RunID:         runID,
		JobName:       "step-2",
		Status:        drevtypes.StatusFailed,
		StartedAt:     time.Now().UTC().Add(-5 * time.Minute),
		FinishedAt:    time.Now().UTC().Add(-2 * time.Minute),
		FailureReason: drevtypes.FailureReasonCommandFailed,
	}); err != nil {
		t.Fatalf("CreateRunJob 2: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updated, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updated.Status != drevtypes.StatusCancelled {
		t.Errorf("status = %s, want %s", updated.Status, drevtypes.StatusCancelled)
	}
	if updated.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("failure_reason = %q, want %q", updated.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if updated.InterruptedStage != drevtypes.StageFinalization {
		t.Errorf("interrupted_stage = %q, want %q", updated.InterruptedStage, drevtypes.StageFinalization)
	}

	// Terminal jobs remain intact
	jobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	for _, j := range jobs {
		if j.JobName == "step-1" && j.Status != drevtypes.StatusSuccess {
			t.Errorf("step-1 status = %s, want success", j.Status)
		}
		if j.JobName == "step-2" && j.Status != drevtypes.StatusFailed {
			t.Errorf("step-2 status = %s, want failed", j.Status)
		}
	}
}

// 5. Ordinary daemon restart (no cancellation intent)
func TestResetGhostRuns_OrdinaryDaemonRestart(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	runID := "run-ordinary-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:           runID,
		PipelineName: "pipeline-ordinary",
		Status:       drevtypes.StatusRunning,
		StartedAt:    time.Now().UTC().Add(-10 * time.Minute),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "job-ord-run-" + uuid.New().String(),
		RunID:     runID,
		JobName:   "running-job",
		Status:    drevtypes.StatusRunning,
		StartedAt: time.Now().UTC().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:      "job-ord-pend-" + uuid.New().String(),
		RunID:   runID,
		JobName: "pending-job",
		Status:  drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:         "job-ord-succ-" + uuid.New().String(),
		RunID:      runID,
		JobName:    "success-job",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  time.Now().UTC().Add(-8 * time.Minute),
		FinishedAt: time.Now().UTC().Add(-6 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updated, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updated.Status != drevtypes.StatusFailed {
		t.Errorf("status = %s, want %s", updated.Status, drevtypes.StatusFailed)
	}
	if updated.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("failure_reason = %q, want %q", updated.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}
	if updated.InterruptedStage != drevtypes.StageExecution {
		t.Errorf("interrupted_stage = %q, want %q", updated.InterruptedStage, drevtypes.StageExecution)
	}

	jobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}

	// Running job -> failed
	if jobMap["running-job"].Status != drevtypes.StatusFailed {
		t.Errorf("running-job status = %s, want failed", jobMap["running-job"].Status)
	}
	if jobMap["running-job"].FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("running-job failure_reason = %q, want %q", jobMap["running-job"].FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}

	// Pending job -> cancelled by daemon restart
	if jobMap["pending-job"].Status != drevtypes.StatusCancelled {
		t.Errorf("pending-job status = %s, want cancelled", jobMap["pending-job"].Status)
	}
	if jobMap["pending-job"].FailureReason != drevtypes.FailureReasonCancelledByDaemonRestart {
		t.Errorf("pending-job failure_reason = %q, want %q", jobMap["pending-job"].FailureReason, drevtypes.FailureReasonCancelledByDaemonRestart)
	}

	// Success job -> untouched
	if jobMap["success-job"].Status != drevtypes.StatusSuccess {
		t.Errorf("success-job status = %s, want success", jobMap["success-job"].Status)
	}
}

// 6. Pending run without outbox
func TestResetGhostRuns_PendingRun_WithoutOutbox(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	// Subcase A: Ordinary pending run without outbox
	runOrdID := "run-pending-ord-" + uuid.New().String()
	runOrd := &drevtypes.Run{
		ID:           runOrdID,
		PipelineName: "pipe",
		Status:       drevtypes.StatusPending,
	}
	if err := s.CreateRun(ctx, runOrd); err != nil {
		t.Fatalf("CreateRun ord: %v", err)
	}

	// Subcase B: Pending run with cancellation intent without outbox
	cancelTime := time.Now().UTC().Add(-1 * time.Minute)
	runCancelID := "run-pending-cancel-" + uuid.New().String()
	runCancel := &drevtypes.Run{
		ID:                      runCancelID,
		PipelineName:            "pipe",
		Status:                  drevtypes.StatusPending,
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, runCancel); err != nil {
		t.Fatalf("CreateRun cancel: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 2 {
		t.Fatalf("expected 2 interrupted runs, got %d (%v)", len(interrupted), interrupted)
	}

	// Verify ordinary pending run
	upOrd, err := s.GetRun(ctx, runOrdID)
	if err != nil {
		t.Fatalf("GetRun ord: %v", err)
	}
	if upOrd.Status != drevtypes.StatusFailed {
		t.Errorf("upOrd status = %s, want failed", upOrd.Status)
	}
	if upOrd.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("upOrd failure_reason = %q, want %q", upOrd.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}
	if upOrd.InterruptedStage != drevtypes.StageScheduling {
		t.Errorf("upOrd interrupted_stage = %q, want %q", upOrd.InterruptedStage, drevtypes.StageScheduling)
	}

	// Verify cancelled pending run
	upCancel, err := s.GetRun(ctx, runCancelID)
	if err != nil {
		t.Fatalf("GetRun cancel: %v", err)
	}
	if upCancel.Status != drevtypes.StatusCancelled {
		t.Errorf("upCancel status = %s, want cancelled", upCancel.Status)
	}
	if upCancel.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("upCancel failure_reason = %q, want %q", upCancel.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if upCancel.InterruptedStage != drevtypes.StageScheduling {
		t.Errorf("upCancel interrupted_stage = %q, want %q", upCancel.InterruptedStage, drevtypes.StageScheduling)
	}
}

// 7. Pending run with outbox
func TestResetGhostRuns_PendingRun_WithOutbox_RemainsPending(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	runID := "run-pending-outbox-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:           runID,
		OrgID:        "org-" + uuid.New().String(),
		PipelineName: "pipe",
		Status:       drevtypes.StatusPending,
	}
	jobs := []*drevtypes.RunJob{
		{
			ID:      "job-pend-" + uuid.New().String(),
			RunID:   runID,
			JobName: "step-1",
			Status:  drevtypes.StatusPending,
		},
	}
	// CreateRunWithLimit creates run, jobs, and pipeline_outbox atomically
	if err := s.CreateRunWithLimit(ctx, run, jobs, 10, "{}"); err != nil {
		t.Fatalf("CreateRunWithLimit: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	for _, id := range interrupted {
		if id == runID {
			t.Fatalf("run with outbox entry was improperly interrupted by ResetGhostRuns: %s", runID)
		}
	}

	upRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if upRun.Status != drevtypes.StatusPending {
		t.Errorf("status = %s, want pending", upRun.Status)
	}

	upJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(upJobs) != 1 || upJobs[0].Status != drevtypes.StatusPending {
		t.Errorf("expected 1 pending job, got %v", upJobs)
	}
}

// 8. Atomic rollback — failure during job update rolls back entire run recovery
func TestResetGhostRuns_AtomicRollback_LeavesNoPartialState(t *testing.T) {
	s, dbPath := newGhostTestStore(t)
	ctx := context.Background()

	cancelTime := time.Now().UTC().Add(-3 * time.Minute)
	runID := "run-rollback-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipe-rollback",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               time.Now().UTC().Add(-10 * time.Minute),
		CancellationRequestedAt: &cancelTime,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	jobID := "job-rollback-" + uuid.New().String()
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        jobID,
		RunID:     runID,
		JobName:   "step-running",
		Status:    drevtypes.StatusRunning,
		StartedAt: time.Now().UTC().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}

	// Install SQLite trigger to fail updates on run_jobs
	directDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open directDB: %v", err)
	}
	t.Cleanup(func() {
		_, _ = directDB.Exec("DROP TRIGGER IF EXISTS fail_ghost_jobs")
		_ = directDB.Close()
	})

	triggerSQL := `
	CREATE TRIGGER fail_ghost_jobs
	BEFORE UPDATE ON run_jobs
	BEGIN
		SELECT RAISE(ABORT, 'simulated job update failure in ghost recovery');
	END;`
	if _, err := directDB.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("failed to install fail_ghost_jobs trigger: %v", err)
	}

	// Attempt ResetGhostRuns: MUST fail because trigger fires when updating run_jobs
	_, err = s.ResetGhostRuns(ctx)
	if err == nil {
		t.Fatalf("expected ResetGhostRuns to fail due to trigger, but it succeeded")
	}
	if !strings.Contains(err.Error(), "simulated job update failure in ghost recovery") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Assert: Rollback ensured NO partial state on the run
	unchangedRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if unchangedRun.Status != drevtypes.StatusRunning {
		t.Errorf("run status = %s, want %s (mutation was not rolled back)", unchangedRun.Status, drevtypes.StatusRunning)
	}
	if unchangedRun.FailureReason != "" {
		t.Errorf("run failure_reason = %q, want empty", unchangedRun.FailureReason)
	}
	if unchangedRun.InterruptedStage != "" {
		t.Errorf("run interrupted_stage = %q, want empty", unchangedRun.InterruptedStage)
	}
	if !unchangedRun.FinishedAt.IsZero() {
		t.Errorf("run finished_at = %v, want zero", unchangedRun.FinishedAt)
	}

	// Assert: Rollback ensured NO partial state on the job
	unchangedJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(unchangedJobs) != 1 || unchangedJobs[0].Status != drevtypes.StatusRunning {
		t.Errorf("job status = %s, want %s", unchangedJobs[0].Status, drevtypes.StatusRunning)
	}

	// Drop trigger and verify clean recovery
	if _, err := directDB.ExecContext(ctx, "DROP TRIGGER IF EXISTS fail_ghost_jobs"); err != nil {
		t.Fatalf("failed to drop trigger: %v", err)
	}

	recoveredIDs, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("subsequent ResetGhostRuns failed: %v", err)
	}
	if len(recoveredIDs) != 1 || recoveredIDs[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, recoveredIDs)
	}

	cleanRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if cleanRun.Status != drevtypes.StatusCancelled {
		t.Errorf("cleanRun status = %s, want %s", cleanRun.Status, drevtypes.StatusCancelled)
	}
	if cleanRun.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("cleanRun failure_reason = %q, want %q", cleanRun.FailureReason, drevtypes.FailureReasonCancelled)
	}
}

// 9. Timestamp preservation
func TestResetGhostRuns_TimestampPreservation(t *testing.T) {
	s, _ := newGhostTestStore(t)
	ctx := context.Background()

	cancelNano := time.Date(2026, 11, 16, 9, 30, 45, 123456789, time.UTC)
	existingRunFin := time.Date(2026, 11, 16, 9, 29, 0, 0, time.UTC)
	existingJobFin := time.Date(2026, 11, 16, 9, 28, 0, 0, time.UTC)

	runID := "run-ts-" + uuid.New().String()
	run := &drevtypes.Run{
		ID:                      runID,
		PipelineName:            "pipe-ts",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               time.Now().UTC().Add(-10 * time.Minute),
		FinishedAt:              existingRunFin,
		CancellationRequestedAt: &cancelNano,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	jobID := "job-ts-" + uuid.New().String()
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:         jobID,
		RunID:      runID,
		JobName:    "step-with-fin",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now().UTC().Add(-5 * time.Minute),
		FinishedAt: existingJobFin,
	}); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	updatedRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.CancellationRequestedAt == nil || !updatedRun.CancellationRequestedAt.Equal(cancelNano) {
		t.Errorf("cancellation_requested_at corrupted: got %v, want %v", updatedRun.CancellationRequestedAt, cancelNano)
	}
	if updatedRun.CancellationRequestedAt.Nanosecond() != cancelNano.Nanosecond() {
		t.Errorf("cancellation_requested_at nanoseconds lost: got %d, want %d",
			updatedRun.CancellationRequestedAt.Nanosecond(), cancelNano.Nanosecond())
	}
	if !updatedRun.FinishedAt.Equal(existingRunFin) {
		t.Errorf("existing run finished_at was overwritten: got %v, want %v", updatedRun.FinishedAt, existingRunFin)
	}

	updatedJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(updatedJobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(updatedJobs))
	}
	if !updatedJobs[0].FinishedAt.Equal(existingJobFin) {
		t.Errorf("existing job finished_at was overwritten: got %v, want %v", updatedJobs[0].FinishedAt, existingJobFin)
	}
}
