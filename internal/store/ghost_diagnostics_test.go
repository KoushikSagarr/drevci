package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func TestResetGhostRuns_JobAndRunDiagnostics(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_diag.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-ghost-diag-1"

	run := &drevtypes.Run{
		ID:           runID,
		PipelineName: "test-pipeline",
		Status:       drevtypes.StatusRunning,
		StartedAt:    time.Now().Add(-10 * time.Minute),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	jobs := []*drevtypes.RunJob{
		{
			ID:        "job-running",
			RunID:     runID,
			JobName:   "step-running",
			Status:    drevtypes.StatusRunning,
			StartedAt: time.Now().Add(-5 * time.Minute),
		},
		{
			ID:        "job-pending",
			RunID:     runID,
			JobName:   "step-pending",
			Status:    drevtypes.StatusPending,
			StartedAt: time.Now().Add(-5 * time.Minute),
		},
		{
			ID:         "job-success",
			RunID:      runID,
			JobName:    "step-success",
			Status:     drevtypes.StatusSuccess,
			StartedAt:  time.Now().Add(-8 * time.Minute),
			FinishedAt: time.Now().Add(-6 * time.Minute),
		},
		{
			ID:            "job-failed",
			RunID:         runID,
			JobName:       "step-failed",
			Status:        drevtypes.StatusFailed,
			StartedAt:     time.Now().Add(-8 * time.Minute),
			FinishedAt:    time.Now().Add(-7 * time.Minute),
			FailureReason: drevtypes.FailureReasonCommandFailed,
		},
	}
	for _, j := range jobs {
		if err := s.CreateRunJob(ctx, j); err != nil {
			t.Fatalf("CreateRunJob(%s): %v", j.ID, err)
		}
	}

	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != runID {
		t.Fatalf("expected [%s], got %v", runID, interrupted)
	}

	// Verify run state
	updatedRun, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != drevtypes.StatusFailed {
		t.Errorf("run status = %s, want failed", updatedRun.Status)
	}
	if updatedRun.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("run failure_reason = %q, want %q", updatedRun.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}
	if updatedRun.InterruptedStage != drevtypes.StageExecution {
		t.Errorf("run interrupted_stage = %q, want %q", updatedRun.InterruptedStage, drevtypes.StageExecution)
	}
	if updatedRun.FinishedAt.IsZero() {
		t.Errorf("run finished_at was not populated")
	}

	// Verify jobs
	jobMap := make(map[string]*drevtypes.RunJob)
	updatedJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	for _, j := range updatedJobs {
		jobMap[j.ID] = j
	}

	// 1. Running job became failed with FailureReasonInterruptedByDaemonRestart
	jRun := jobMap["job-running"]
	if jRun.Status != drevtypes.StatusFailed {
		t.Errorf("job-running status = %s, want failed", jRun.Status)
	}
	if jRun.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("job-running failure_reason = %q, want %q", jRun.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}
	if jRun.FinishedAt.IsZero() {
		t.Errorf("job-running finished_at was not populated")
	}

	// 2. Pending job became cancelled with FailureReasonCancelledByDaemonRestart
	jPend := jobMap["job-pending"]
	if jPend.Status != drevtypes.StatusCancelled {
		t.Errorf("job-pending status = %s, want cancelled", jPend.Status)
	}
	if jPend.FailureReason != drevtypes.FailureReasonCancelledByDaemonRestart {
		t.Errorf("job-pending failure_reason = %q, want %q", jPend.FailureReason, drevtypes.FailureReasonCancelledByDaemonRestart)
	}
	if jPend.FinishedAt.IsZero() {
		t.Errorf("job-pending finished_at was not populated")
	}

	// 3. Success job remained unchanged
	jSucc := jobMap["job-success"]
	if jSucc.Status != drevtypes.StatusSuccess {
		t.Errorf("job-success status = %s, want success", jSucc.Status)
	}
	if jSucc.FailureReason != "" {
		t.Errorf("job-success failure_reason = %q, want empty", jSucc.FailureReason)
	}

	// 4. Failed job remained unchanged with original failure reason preserved
	jFail := jobMap["job-failed"]
	if jFail.Status != drevtypes.StatusFailed {
		t.Errorf("job-failed status = %s, want failed", jFail.Status)
	}
	if jFail.FailureReason != drevtypes.FailureReasonCommandFailed {
		t.Errorf("job-failed failure_reason = %q, want %q", jFail.FailureReason, drevtypes.FailureReasonCommandFailed)
	}
}

func TestResetGhostRuns_StageInferenceScenarios(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_stages.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Scenario 1: Pending run, 0 jobs, no outbox -> scheduling
	run1 := &drevtypes.Run{ID: "run-sched", PipelineName: "pipe", Status: drevtypes.StatusPending}
	if err := s.CreateRun(ctx, run1); err != nil {
		t.Fatalf("CreateRun run1: %v", err)
	}

	started := time.Now().Add(-10 * time.Minute)

	// Scenario 2: Running run, 0 jobs -> preparation
	run2 := &drevtypes.Run{ID: "run-prep-nojobs", PipelineName: "pipe", Status: drevtypes.StatusRunning, StartedAt: started}
	if err := s.CreateRun(ctx, run2); err != nil {
		t.Fatalf("CreateRun run2: %v", err)
	}

	// Scenario 3: Running run, all jobs pending -> preparation
	run3 := &drevtypes.Run{ID: "run-prep-allpending", PipelineName: "pipe", Status: drevtypes.StatusRunning, StartedAt: started}
	if err := s.CreateRun(ctx, run3); err != nil {
		t.Fatalf("CreateRun run3: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{ID: "j3-1", RunID: run3.ID, JobName: "build", Status: drevtypes.StatusPending}); err != nil {
		t.Fatalf("CreateRunJob j3-1: %v", err)
	}

	// Scenario 4: Running run, 1 running, 1 pending -> execution
	run4 := &drevtypes.Run{ID: "run-exec", PipelineName: "pipe", Status: drevtypes.StatusRunning, StartedAt: started}
	if err := s.CreateRun(ctx, run4); err != nil {
		t.Fatalf("CreateRun run4: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{ID: "j4-1", RunID: run4.ID, JobName: "build", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRunJob j4-1: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{ID: "j4-2", RunID: run4.ID, JobName: "test", Status: drevtypes.StatusPending}); err != nil {
		t.Fatalf("CreateRunJob j4-2: %v", err)
	}

	// Scenario 5: Running run, all jobs terminal -> finalization
	run5 := &drevtypes.Run{ID: "run-fin", PipelineName: "pipe", Status: drevtypes.StatusRunning, StartedAt: started}
	if err := s.CreateRun(ctx, run5); err != nil {
		t.Fatalf("CreateRun run5: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{ID: "j5-1", RunID: run5.ID, JobName: "build", Status: drevtypes.StatusSuccess}); err != nil {
		t.Fatalf("CreateRunJob j5-1: %v", err)
	}
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{ID: "j5-2", RunID: run5.ID, JobName: "test", Status: drevtypes.StatusFailed}); err != nil {
		t.Fatalf("CreateRunJob j5-2: %v", err)
	}

	// Execute ResetGhostRuns
	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 5 {
		t.Fatalf("expected 5 interrupted runs, got %d (%v)", len(interrupted), interrupted)
	}

	tests := []struct {
		runID     string
		wantStage string
	}{
		{"run-sched", drevtypes.StageScheduling},
		{"run-prep-nojobs", drevtypes.StagePreparation},
		{"run-prep-allpending", drevtypes.StagePreparation},
		{"run-exec", drevtypes.StageExecution},
		{"run-fin", drevtypes.StageFinalization},
	}

	for _, tt := range tests {
		r, err := s.GetRun(ctx, tt.runID)
		if err != nil {
			t.Errorf("GetRun(%s): %v", tt.runID, err)
			continue
		}
		if r.InterruptedStage != tt.wantStage {
			t.Errorf("run %s: InterruptedStage = %q, want %q", tt.runID, r.InterruptedStage, tt.wantStage)
		}
		if r.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
			t.Errorf("run %s: FailureReason = %q, want %q", tt.runID, r.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
		}
		if r.Status != drevtypes.StatusFailed {
			t.Errorf("run %s: Status = %s, want failed", tt.runID, r.Status)
		}
	}
}

func TestResetGhostRuns_ConcurrencyAndIdempotency(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_concurrency.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	numRuns := 10

	for i := 0; i < numRuns; i++ {
		id := fmt.Sprintf("run-concurrent-%d", i)
		r := &drevtypes.Run{
			ID:           id,
			PipelineName: "pipe",
			Status:       drevtypes.StatusRunning,
			StartedAt:    time.Now().Add(-5 * time.Minute),
		}
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
			ID:      fmt.Sprintf("job-concurrent-%d", i),
			RunID:   id,
			JobName: "test",
			Status:  drevtypes.StatusRunning,
		}); err != nil {
			t.Fatalf("CreateRunJob: %v", err)
		}
	}

	var wg sync.WaitGroup
	workers := 10
	results := make([][]string, workers)
	errs := make([]error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			res, err := s.ResetGhostRuns(ctx)
			results[workerID] = res
			errs[workerID] = err
		}(w)
	}
	wg.Wait()

	for w, err := range errs {
		if err != nil {
			t.Errorf("worker %d returned error: %v", w, err)
		}
	}

	claimed := make(map[string]int)
	for _, res := range results {
		for _, id := range res {
			claimed[id]++
		}
	}

	// Exactly numRuns runs must be reset across all workers, each exactly once
	if len(claimed) != numRuns {
		t.Errorf("expected %d unique runs claimed, got %d", numRuns, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("run %s claimed %d times (expected 1)", id, count)
		}
	}

	// Subsequent invocation must be idempotent and return 0 runs
	subsequent, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("subsequent ResetGhostRuns: %v", err)
	}
	if len(subsequent) != 0 {
		t.Errorf("expected 0 runs on subsequent call, got %d", len(subsequent))
	}
}

func TestGetRunStats_ExcludesInterruptedRunsFromMeanDuration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_stats.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// Run A: success, 10 seconds duration
	runA := &drevtypes.Run{
		ID:           "run-a",
		PipelineName: "pipe",
		Status:       drevtypes.StatusSuccess,
		StartedAt:    now.Add(-100 * time.Second),
		FinishedAt:   now.Add(-90 * time.Second),
	}
	if err := s.CreateRun(ctx, runA); err != nil {
		t.Fatalf("CreateRun A: %v", err)
	}

	// Run B: success, 20 seconds duration
	runB := &drevtypes.Run{
		ID:           "run-b",
		PipelineName: "pipe",
		Status:       drevtypes.StatusSuccess,
		StartedAt:    now.Add(-80 * time.Second),
		FinishedAt:   now.Add(-60 * time.Second),
	}
	if err := s.CreateRun(ctx, runB); err != nil {
		t.Fatalf("CreateRun B: %v", err)
	}

	// Run C: normal failed, 30 seconds duration
	runC := &drevtypes.Run{
		ID:           "run-c",
		PipelineName: "pipe",
		Status:       drevtypes.StatusFailed,
		StartedAt:    now.Add(-50 * time.Second),
		FinishedAt:   now.Add(-20 * time.Second),
	}
	if err := s.CreateRun(ctx, runC); err != nil {
		t.Fatalf("CreateRun C: %v", err)
	}

	// Run D: daemon-interrupted run with huge duration (100,000s)
	runD := &drevtypes.Run{
		ID:               "run-d",
		PipelineName:     "pipe",
		Status:           drevtypes.StatusFailed,
		FailureReason:    drevtypes.FailureReasonInterruptedByDaemonRestart,
		InterruptedStage: drevtypes.StageExecution,
		StartedAt:        now.Add(-100000 * time.Second),
		FinishedAt:       now,
	}
	if err := s.CreateRun(ctx, runD); err != nil {
		t.Fatalf("CreateRun D: %v", err)
	}

	stats, err := s.GetRunStats(ctx, "", 25)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}

	// Total completed runs sample count must include run D: 4
	if stats.CompletedSampleCount != 4 {
		t.Errorf("CompletedSampleCount = %d, want 4", stats.CompletedSampleCount)
	}
	if stats.SucceededCount != 2 {
		t.Errorf("SucceededCount = %d, want 2", stats.SucceededCount)
	}
	if stats.FailedCount != 2 {
		t.Errorf("FailedCount = %d, want 2", stats.FailedCount)
	}

	// Duration sample count must EXCLUDE run D: 3
	if stats.DurationSampleCount != 3 {
		t.Errorf("DurationSampleCount = %d, want 3", stats.DurationSampleCount)
	}

	// MeanDurationSec must be (10 + 20 + 30) / 3 = 20
	if stats.MeanDurationSec == nil {
		t.Fatalf("MeanDurationSec is nil")
	}
	if *stats.MeanDurationSec != 20 {
		t.Errorf("MeanDurationSec = %d, want 20", *stats.MeanDurationSec)
	}
}

func TestGetRunStats_AllRunsInterrupted_MeanDurationIsNil(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_stats_nil.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	runD := &drevtypes.Run{
		ID:               "run-d",
		PipelineName:     "pipe",
		Status:           drevtypes.StatusFailed,
		FailureReason:    drevtypes.FailureReasonInterruptedByDaemonRestart,
		InterruptedStage: drevtypes.StageExecution,
		StartedAt:        now.Add(-1000 * time.Second),
		FinishedAt:       now,
	}
	if err := s.CreateRun(ctx, runD); err != nil {
		t.Fatalf("CreateRun D: %v", err)
	}

	stats, err := s.GetRunStats(ctx, "", 25)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}

	if stats.CompletedSampleCount != 1 {
		t.Errorf("CompletedSampleCount = %d, want 1", stats.CompletedSampleCount)
	}
	if stats.DurationSampleCount != 0 {
		t.Errorf("DurationSampleCount = %d, want 0", stats.DurationSampleCount)
	}
	if stats.MeanDurationSec != nil {
		t.Errorf("MeanDurationSec = %v, want nil", *stats.MeanDurationSec)
	}
}
