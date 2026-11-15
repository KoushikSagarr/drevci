package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func TestGetRunLogs_Scenario1_InterruptedRunWithLogFile(t *testing.T) {
	srv, h := setupTestServer(t)

	runID := "run-interrupted-with-log"
	finishedAt := time.Date(2026, 11, 15, 10, 30, 0, 0, time.UTC)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:               runID,
		PipelineID:       "pipe-1",
		Status:           drevtypes.StatusFailed,
		FailureReason:    drevtypes.FailureReasonInterruptedByDaemonRestart,
		InterruptedStage: drevtypes.StageExecution,
		StartedAt:        finishedAt.Add(-5 * time.Minute),
		FinishedAt:       finishedAt,
	}
	if err := h.store.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Write log file to logDir
	logFile := filepath.Join(h.logDir, runID+".log")
	if err := os.MkdirAll(h.logDir, 0755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	logContent := "step 1: downloading dependencies\nstep 2: running test\n"
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/"+runID+"/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "data: step 1: downloading dependencies") {
		t.Errorf("body missing first log line, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "data: step 2: running test") {
		t.Errorf("body missing second log line, got:\n%s", bodyStr)
	}
	expectedDiagnostic := `data: [drev] RUN INTERRUPTED: Pipeline execution was interrupted by daemon termination during stage "execution". Recovered at 2026-11-15T10:30:00Z.`
	if !strings.Contains(bodyStr, expectedDiagnostic) {
		t.Errorf("body missing recovery diagnostic message\nwant contains: %s\ngot:\n%s", expectedDiagnostic, bodyStr)
	}
}

func TestGetRunLogs_Scenario2_InterruptedRunWithoutLogFile(t *testing.T) {
	srv, h := setupTestServer(t)

	runID := "run-interrupted-no-log"
	finishedAt := time.Date(2026, 11, 15, 10, 30, 0, 0, time.UTC)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:               runID,
		PipelineID:       "pipe-2",
		Status:           drevtypes.StatusFailed,
		FailureReason:    drevtypes.FailureReasonInterruptedByDaemonRestart,
		InterruptedStage: drevtypes.StagePreparation,
		StartedAt:        finishedAt.Add(-2 * time.Minute),
		FinishedAt:       finishedAt,
	}
	if err := h.store.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Ensure no log file exists
	logFile := filepath.Join(h.logDir, runID+".log")
	os.Remove(logFile)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/"+runID+"/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(body)

	expectedDiagnostic := `data: [drev] RUN INTERRUPTED: Pipeline execution was interrupted by daemon termination during stage "preparation". Recovered at 2026-11-15T10:30:00Z.`
	if !strings.Contains(bodyStr, expectedDiagnostic) {
		t.Errorf("body missing recovery diagnostic message\nwant contains: %s\ngot:\n%s", expectedDiagnostic, bodyStr)
	}
}

func TestGetRunLogs_Scenario3_InterruptedRunWithEmptyLogFile(t *testing.T) {
	srv, h := setupTestServer(t)

	runID := "run-interrupted-empty-log"
	finishedAt := time.Date(2026, 11, 15, 10, 30, 0, 0, time.UTC)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:               runID,
		PipelineID:       "pipe-3",
		Status:           drevtypes.StatusFailed,
		FailureReason:    drevtypes.FailureReasonInterruptedByDaemonRestart,
		InterruptedStage: drevtypes.StageScheduling,
		StartedAt:        finishedAt.Add(-1 * time.Minute),
		FinishedAt:       finishedAt,
	}
	if err := h.store.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Create 0-byte log file
	if err := os.MkdirAll(h.logDir, 0755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	logFile := filepath.Join(h.logDir, runID+".log")
	if err := os.WriteFile(logFile, []byte{}, 0644); err != nil {
		t.Fatalf("write empty log file: %v", err)
	}

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/"+runID+"/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(body)

	expectedDiagnostic := `data: [drev] RUN INTERRUPTED: Pipeline execution was interrupted by daemon termination during stage "scheduling". Recovered at 2026-11-15T10:30:00Z.`
	if !strings.Contains(bodyStr, expectedDiagnostic) {
		t.Errorf("body missing recovery diagnostic message\nwant contains: %s\ngot:\n%s", expectedDiagnostic, bodyStr)
	}
}

func TestGetRunLogs_Scenario4_OrdinaryCompletedRunWithoutLogFile(t *testing.T) {
	srv, h := setupTestServer(t)

	runID := "run-ordinary-completed"
	finishedAt := time.Date(2026, 11, 15, 10, 30, 0, 0, time.UTC)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-4",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  finishedAt.Add(-1 * time.Minute),
		FinishedAt: finishedAt,
	}
	if err := h.store.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Ensure no log file exists
	logFile := filepath.Join(h.logDir, runID+".log")
	os.Remove(logFile)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/"+runID+"/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(body)

	if strings.Contains(bodyStr, "RUN INTERRUPTED") {
		t.Errorf("ordinary completed run should not emit RUN INTERRUPTED, got:\n%s", bodyStr)
	}
	if len(bodyStr) != 0 {
		t.Errorf("expected empty body for run without logs, got %q", bodyStr)
	}
}

func TestGetRunLogs_Scenario5_NonExistentRun_Returns404(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/non-existent-run-id/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 Not Found", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "text/event-stream") {
		t.Errorf("Content-Type should not be text/event-stream for 404, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "run not found") {
		t.Errorf("body missing 'run not found', got %q", string(body))
	}
}

func TestGetRunLogs_Scenario6_Unauthorized_Returns401(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/any-run/logs", nil)
	// No Authorization header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 Unauthorized", resp.StatusCode)
	}
}

func TestTrigger_InterruptedRun_ReTriggerSucceeds(t *testing.T) {
	srv, h := setupTestServer(t)

	pipelineYAML := `
name: retrigger-test
jobs:
  - name: step1
    image: alpine
    steps:
      - name: test
        run: echo hi
`
	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("write yml: %v", err)
	}

	payload := fmt.Sprintf(`{"pipeline_path": %q}`, ymlPath)

	// Trigger run 1
	req1, _ := http.NewRequest("POST", srv.URL+"/api/v1/pipelines/trigger", strings.NewReader(payload))
	req1.Header.Set("Authorization", "Bearer test-token")
	req1.Header.Set("Content-Type", "application/json")

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("trigger run 1: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("trigger 1 status = %d, want 202", resp1.StatusCode)
	}

	// Drain the queue to simulate worker claiming run 1
	job := <-h.queue.Drain()
	if job == nil {
		t.Fatalf("expected job in queue")
	}
	run1ID := job.RunID

	// Mark run 1 as running with a job
	ctx := context.Background()
	_ = h.store.UpdateRunStatus(ctx, run1ID, drevtypes.StatusRunning)
	_ = h.store.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:        "job-1",
		RunID:     run1ID,
		JobName:   "step1",
		Status:    drevtypes.StatusRunning,
		StartedAt: time.Now(),
	})

	// Daemon crashes/restarts -> ResetGhostRuns
	interrupted, err := h.store.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != run1ID {
		t.Fatalf("expected [%s] interrupted, got %v", run1ID, interrupted)
	}

	// Verify run 1 is failed with diagnostics
	r1, err := h.store.GetRun(ctx, run1ID)
	if err != nil {
		t.Fatalf("GetRun r1: %v", err)
	}
	if r1.Status != drevtypes.StatusFailed {
		t.Errorf("r1 status = %s, want failed", r1.Status)
	}
	if r1.FailureReason != drevtypes.FailureReasonInterruptedByDaemonRestart {
		t.Errorf("r1 failure_reason = %q, want %q", r1.FailureReason, drevtypes.FailureReasonInterruptedByDaemonRestart)
	}
	if r1.InterruptedStage != drevtypes.StageExecution {
		t.Errorf("r1 stage = %q, want %q", r1.InterruptedStage, drevtypes.StageExecution)
	}

	// Re-trigger the same pipeline
	req2, _ := http.NewRequest("POST", srv.URL+"/api/v1/pipelines/trigger", strings.NewReader(payload))
	req2.Header.Set("Authorization", "Bearer test-token")
	req2.Header.Set("Content-Type", "application/json")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("re-trigger: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("re-trigger status = %d, want 202", resp2.StatusCode)
	}

	// Verify run 2 is enqueued and distinct from run 1
	job2 := <-h.queue.Drain()
	if job2 == nil {
		t.Fatalf("expected second job in queue")
	}
	if job2.RunID == run1ID {
		t.Fatalf("expected new run ID for re-triggered pipeline, got same %s", run1ID)
	}

	r2, err := h.store.GetRun(ctx, job2.RunID)
	if err != nil {
		t.Fatalf("GetRun r2: %v", err)
	}
	if r2.Status != drevtypes.StatusPending {
		t.Errorf("r2 status = %s, want pending", r2.Status)
	}
	if r2.FailureReason != "" {
		t.Errorf("r2 failure_reason = %q, want empty", r2.FailureReason)
	}
}
