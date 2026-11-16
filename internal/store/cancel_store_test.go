package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
	_ "modernc.org/sqlite"
)

// 1. Pending run cancellation transitions run to cancelled, sets failure_reason,
// interrupted_stage = scheduling, finished_at, and cancellation_requested_at.
func TestCancelRun_PendingRun_TransitionsToCancelled(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_pending.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-pending-1"
	orgID := "org-test"

	run := &drevtypes.Run{
		ID:          runID,
		OrgID:       orgID,
		PipelineID:  "pipe-1",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	job1 := &drevtypes.RunJob{
		ID:      "job-p1",
		RunID:   runID,
		JobName: "build",
		Status:  drevtypes.StatusPending,
	}
	job2 := &drevtypes.RunJob{
		ID:      "job-p2",
		RunID:   runID,
		JobName: "test",
		Status:  drevtypes.StatusPending,
	}
	if err := s.CreateRunJob(ctx, job1); err != nil {
		t.Fatalf("CreateRunJob(1) error = %v", err)
	}
	if err := s.CreateRunJob(ctx, job2); err != nil {
		t.Fatalf("CreateRunJob(2) error = %v", err)
	}

	res, err := s.CancelRun(ctx, runID, orgID)
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}

	if res.PreviousStatus != drevtypes.StatusPending {
		t.Errorf("expected PreviousStatus %s, got %s", drevtypes.StatusPending, res.PreviousStatus)
	}
	if res.WasRunning {
		t.Errorf("expected WasRunning = false, got true")
	}
	if res.AlreadyDone {
		t.Errorf("expected AlreadyDone = false, got true")
	}
	if res.AlreadyPending {
		t.Errorf("expected AlreadyPending = false, got true")
	}
	if res.Run.Status != drevtypes.StatusCancelled {
		t.Errorf("expected Run.Status %s, got %s", drevtypes.StatusCancelled, res.Run.Status)
	}
	if res.Run.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("expected FailureReason %q, got %q", drevtypes.FailureReasonCancelled, res.Run.FailureReason)
	}
	if res.Run.InterruptedStage != drevtypes.StageScheduling {
		t.Errorf("expected InterruptedStage %q, got %q", drevtypes.StageScheduling, res.Run.InterruptedStage)
	}
	if res.Run.FinishedAt.IsZero() {
		t.Errorf("expected FinishedAt to be set, got zero")
	}
	if res.Run.CancellationRequestedAt == nil {
		t.Fatalf("expected CancellationRequestedAt to be set, got nil")
	}

	// Verify database persistence via GetRun
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if persisted.Status != drevtypes.StatusCancelled {
		t.Errorf("persisted Status = %s, want %s", persisted.Status, drevtypes.StatusCancelled)
	}
	if persisted.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("persisted FailureReason = %q, want %q", persisted.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if persisted.InterruptedStage != drevtypes.StageScheduling {
		t.Errorf("persisted InterruptedStage = %q, want %q", persisted.InterruptedStage, drevtypes.StageScheduling)
	}
	if persisted.FinishedAt.IsZero() {
		t.Errorf("persisted FinishedAt is zero")
	}
	if persisted.CancellationRequestedAt == nil {
		t.Errorf("persisted CancellationRequestedAt is nil")
	}

	// 2. Pending jobs become cancelled with failure_reason='cancelled' and finished_at
	for _, j := range persisted.Jobs {
		if j.Status != drevtypes.StatusCancelled {
			t.Errorf("job %s status = %s, want %s", j.ID, j.Status, drevtypes.StatusCancelled)
		}
		if j.FailureReason != drevtypes.FailureReasonCancelled {
			t.Errorf("job %s failure_reason = %q, want %q", j.ID, j.FailureReason, drevtypes.FailureReasonCancelled)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("job %s finished_at is zero", j.ID)
		}
	}
}

// 3. Existing terminal jobs (success, failed, skipped) remain unchanged on pending run cancel.
func TestCancelRun_PendingRun_PreservesTerminalJobs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_terminal_jobs.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-term-jobs"

	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-term",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	tSuccess := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	tFailed := time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)
	tSkipped := time.Date(2026, 11, 15, 12, 10, 0, 0, time.UTC)

	jobs := []*drevtypes.RunJob{
		{ID: "j-succ", RunID: runID, JobName: "build", Status: drevtypes.StatusSuccess, FinishedAt: tSuccess},
		{ID: "j-fail", RunID: runID, JobName: "lint", Status: drevtypes.StatusFailed, FailureReason: "linter_error", FinishedAt: tFailed},
		{ID: "j-skip", RunID: runID, JobName: "deploy", Status: drevtypes.StatusSkipped, FinishedAt: tSkipped},
		{ID: "j-pend", RunID: runID, JobName: "test", Status: drevtypes.StatusPending},
	}
	for _, j := range jobs {
		if err := s.CreateRunJob(ctx, j); err != nil {
			t.Fatalf("CreateRunJob(%s) error = %v", j.ID, err)
		}
	}

	res, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if res.Run.Status != drevtypes.StatusCancelled {
		t.Fatalf("expected run cancelled, got %s", res.Run.Status)
	}

	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}

	jobMap := make(map[string]drevtypes.RunJob)
	for _, j := range persisted.Jobs {
		jobMap[j.ID] = j
	}

	// Verify terminal jobs preserved
	succ := jobMap["j-succ"]
	if succ.Status != drevtypes.StatusSuccess {
		t.Errorf("success job status changed to %s", succ.Status)
	}
	if succ.FinishedAt.Unix() != tSuccess.Unix() {
		t.Errorf("success job finished_at altered: got %v, want %v", succ.FinishedAt, tSuccess)
	}

	fail := jobMap["j-fail"]
	if fail.Status != drevtypes.StatusFailed {
		t.Errorf("failed job status changed to %s", fail.Status)
	}
	if fail.FailureReason != "linter_error" {
		t.Errorf("failed job failure_reason altered: got %q, want %q", fail.FailureReason, "linter_error")
	}
	if fail.FinishedAt.Unix() != tFailed.Unix() {
		t.Errorf("failed job finished_at altered: got %v, want %v", fail.FinishedAt, tFailed)
	}

	skip := jobMap["j-skip"]
	if skip.Status != drevtypes.StatusSkipped {
		t.Errorf("skipped job status changed to %s", skip.Status)
	}
	if skip.FinishedAt.Unix() != tSkipped.Unix() {
		t.Errorf("skipped job finished_at altered: got %v, want %v", skip.FinishedAt, tSkipped)
	}

	pend := jobMap["j-pend"]
	if pend.Status != drevtypes.StatusCancelled {
		t.Errorf("pending job did not cancel, status = %s", pend.Status)
	}
	if pend.FailureReason != drevtypes.FailureReasonCancelled {
		t.Errorf("pending job failure_reason = %q, want %q", pend.FailureReason, drevtypes.FailureReasonCancelled)
	}
	if pend.FinishedAt.IsZero() {
		t.Errorf("pending job finished_at was not set")
	}
}

// 4. Pending outbox entry is deleted.
// 5. webhook_deliveries record is preserved; duplicate webhook delivery returns ErrDuplicateDelivery.
func TestCancelRun_PendingRun_PurgesOutbox_PreservesWebhookDelivery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_outbox_webhook.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-webhook-cancel"
	deliveryID := "del-gh-42"

	delivery := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "acme/widgets",
		PRNumber:   0,
		RunID:      runID,
	}

	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-webhook",
		Status:     drevtypes.StatusPending,
	}

	jobs := []*drevtypes.RunJob{
		{ID: "j-wh-1", RunID: runID, JobName: "build", Status: drevtypes.StatusPending},
	}

	if err := s.CreateRunWithDelivery(ctx, delivery, run, jobs, "pipeline: webhook-spec"); err != nil {
		t.Fatalf("CreateRunWithDelivery() error = %v", err)
	}

	// Verify outbox has entry
	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox() error = %v", err)
	}
	foundOutbox := false
	for _, it := range outboxItems {
		if it.RunID == runID {
			foundOutbox = true
			break
		}
	}
	if !foundOutbox {
		t.Fatalf("expected run %s to be in outbox before cancellation", runID)
	}

	// Verify delivery is recorded
	gotDel, err := s.GetDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatalf("GetDelivery() error = %v", err)
	}
	if gotDel == nil || gotDel.DeliveryID != deliveryID {
		t.Fatalf("expected delivery %s to exist before cancellation", deliveryID)
	}

	// Cancel the pending run
	res, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if res.Run.Status != drevtypes.StatusCancelled {
		t.Fatalf("expected run status cancelled, got %s", res.Run.Status)
	}

	// Verify outbox entry is DELETED
	outboxAfter, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox() error = %v", err)
	}
	for _, it := range outboxAfter {
		if it.RunID == runID {
			t.Errorf("outbox entry for run %s was NOT deleted", runID)
		}
	}

	// Verify webhook delivery is PRESERVED
	delAfter, err := s.GetDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatalf("GetDelivery() after cancel error = %v", err)
	}
	if delAfter == nil || delAfter.DeliveryID != deliveryID {
		t.Errorf("webhook delivery was deleted, expected preserved")
	}

	// Duplicate delivery rejected
	dupErr := s.RecordDelivery(ctx, &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "acme/widgets",
		RunID:      "run-different",
	})
	if !errors.Is(dupErr, store.ErrDuplicateDelivery) {
		t.Errorf("expected ErrDuplicateDelivery on replayed delivery, got %v", dupErr)
	}
}

// 6. Running run receives cancellation_requested_at, remains running, jobs remain untouched.
func TestCancelRun_RunningRun_RecordsCancellationIntent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_running.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-running-1"

	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-running",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	job1 := &drevtypes.RunJob{
		ID:        "j-run-1",
		RunID:     runID,
		JobName:   "compile",
		Status:    drevtypes.StatusRunning,
		StartedAt: time.Now().UTC(),
	}
	job2 := &drevtypes.RunJob{
		ID:      "j-run-2",
		RunID:   runID,
		JobName: "integration",
		Status:  drevtypes.StatusPending,
	}
	if err := s.CreateRunJob(ctx, job1); err != nil {
		t.Fatalf("CreateRunJob(1) error = %v", err)
	}
	if err := s.CreateRunJob(ctx, job2); err != nil {
		t.Fatalf("CreateRunJob(2) error = %v", err)
	}

	// Transition run to running
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("UpdateRunStatus() error = %v", err)
	}

	res, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}

	if res.PreviousStatus != drevtypes.StatusRunning {
		t.Errorf("expected PreviousStatus %s, got %s", drevtypes.StatusRunning, res.PreviousStatus)
	}
	if !res.WasRunning {
		t.Errorf("expected WasRunning = true, got false")
	}
	if res.AlreadyPending {
		t.Errorf("expected AlreadyPending = false, got true")
	}
	if res.AlreadyDone {
		t.Errorf("expected AlreadyDone = false, got true")
	}
	if res.Run.Status != drevtypes.StatusRunning {
		t.Errorf("expected Run.Status %s, got %s", drevtypes.StatusRunning, res.Run.Status)
	}
	if res.Run.CancellationRequestedAt == nil {
		t.Fatalf("expected CancellationRequestedAt to be set, got nil")
	}

	// Verify database persistence via GetRun
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if persisted.Status != drevtypes.StatusRunning {
		t.Errorf("persisted Status = %s, want %s", persisted.Status, drevtypes.StatusRunning)
	}
	if persisted.CancellationRequestedAt == nil {
		t.Fatalf("persisted CancellationRequestedAt is nil")
	}

	// Jobs must remain untouched in Milestone 2
	for _, j := range persisted.Jobs {
		if j.ID == "j-run-1" && j.Status != drevtypes.StatusRunning {
			t.Errorf("job 1 status modified: got %s, want running", j.Status)
		}
		if j.ID == "j-run-2" && j.Status != drevtypes.StatusPending {
			t.Errorf("job 2 status modified: got %s, want pending", j.Status)
		}
	}
}

// 7. Repeated cancellation of running run is idempotent and does not overwrite original timestamp.
func TestCancelRun_RunningRun_RepeatedCancellationIsIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_repeat_running.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-running-repeat"

	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-repeat",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("UpdateRunStatus() error = %v", err)
	}

	// First cancel request
	res1, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("first CancelRun() error = %v", err)
	}
	if !res1.WasRunning || res1.AlreadyPending {
		t.Fatalf("first cancel result unexpected: %+v", res1)
	}
	tFirst := *res1.Run.CancellationRequestedAt

	time.Sleep(20 * time.Millisecond)

	// Second cancel request (idempotent)
	res2, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("second CancelRun() error = %v", err)
	}
	if res2.PreviousStatus != drevtypes.StatusRunning {
		t.Errorf("expected PreviousStatus %s, got %s", drevtypes.StatusRunning, res2.PreviousStatus)
	}
	if !res2.WasRunning {
		t.Errorf("expected WasRunning = true, got false")
	}
	if !res2.AlreadyPending {
		t.Errorf("expected AlreadyPending = true, got false")
	}
	if res2.AlreadyDone {
		t.Errorf("expected AlreadyDone = false, got true")
	}

	tSecond := *res2.Run.CancellationRequestedAt
	if !tSecond.Equal(tFirst) {
		t.Errorf("cancellation timestamp was overwritten: first=%v, second=%v", tFirst, tSecond)
	}
}

// 8. Already-cancelled run is idempotent and returns AlreadyDone: true.
func TestCancelRun_AlreadyCancelledRun_IsIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_already_cancelled.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-already-cancelled"

	now := time.Now().UTC()
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:                      runID,
		PipelineID:              "pipe-cancelled",
		Status:                  drevtypes.StatusCancelled,
		FailureReason:           drevtypes.FailureReasonCancelled,
		InterruptedStage:        drevtypes.StageExecution,
		FinishedAt:              now,
		CancellationRequestedAt: &now,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	res, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if res.PreviousStatus != drevtypes.StatusCancelled {
		t.Errorf("expected PreviousStatus %s, got %s", drevtypes.StatusCancelled, res.PreviousStatus)
	}
	if !res.AlreadyDone {
		t.Errorf("expected AlreadyDone = true, got false")
	}
	if res.WasRunning {
		t.Errorf("expected WasRunning = false, got true")
	}
	if res.AlreadyPending {
		t.Errorf("expected AlreadyPending = false, got true")
	}
	if res.Run.Status != drevtypes.StatusCancelled {
		t.Errorf("expected Run.Status %s, got %s", drevtypes.StatusCancelled, res.Run.Status)
	}
}

// 9. Successful run returns ErrRunAlreadyTerminal without mutation.
// 10. Failed run returns ErrRunAlreadyTerminal without mutation.
func TestCancelRun_TerminalRun_ReturnsErrRunAlreadyTerminal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_terminal.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		runID := "run-term-succ"
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-succ",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun() error = %v", err)
		}
		if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
			t.Fatalf("UpdateRunStatus(running) error = %v", err)
		}
		if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess); err != nil {
			t.Fatalf("UpdateRunStatus() error = %v", err)
		}

		res, err := s.CancelRun(ctx, runID, "")
		if res != nil {
			t.Errorf("expected nil result on terminal run, got %+v", res)
		}
		if !errors.Is(err, store.ErrRunAlreadyTerminal) {
			t.Fatalf("expected ErrRunAlreadyTerminal, got %v", err)
		}

		persisted, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun() error = %v", err)
		}
		if persisted.Status != drevtypes.StatusSuccess {
			t.Errorf("status was mutated to %s", persisted.Status)
		}
		if persisted.CancellationRequestedAt != nil {
			t.Errorf("cancellation_requested_at was populated on successful run")
		}
	})

	t.Run("failed", func(t *testing.T) {
		runID := "run-term-failed"
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-failed",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun() error = %v", err)
		}
		if err := s.UpdateRunStatusWithDiagnostics(ctx, runID, drevtypes.StatusFailed, "build_failure", "execution"); err != nil {
			t.Fatalf("UpdateRunStatus() error = %v", err)
		}

		res, err := s.CancelRun(ctx, runID, "")
		if res != nil {
			t.Errorf("expected nil result on terminal run, got %+v", res)
		}
		if !errors.Is(err, store.ErrRunAlreadyTerminal) {
			t.Fatalf("expected ErrRunAlreadyTerminal, got %v", err)
		}

		persisted, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun() error = %v", err)
		}
		if persisted.Status != drevtypes.StatusFailed {
			t.Errorf("status was mutated to %s", persisted.Status)
		}
		if persisted.FailureReason != "build_failure" {
			t.Errorf("failure_reason altered: %q", persisted.FailureReason)
		}
	})
}

// 11. Unknown run ID returns ErrRunNotFound.
func TestCancelRun_UnknownRunID_ReturnsErrRunNotFound(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_not_found.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	t.Run("non-existent uuid", func(t *testing.T) {
		res, err := s.CancelRun(ctx, "00000000-0000-0000-0000-000000000000", "")
		if res != nil {
			t.Errorf("expected nil result, got %+v", res)
		}
		if !errors.Is(err, store.ErrRunNotFound) {
			t.Fatalf("expected ErrRunNotFound, got %v", err)
		}
	})

	t.Run("empty runID", func(t *testing.T) {
		res, err := s.CancelRun(ctx, "", "")
		if res != nil {
			t.Errorf("expected nil result, got %+v", res)
		}
		if !errors.Is(err, store.ErrRunNotFound) {
			t.Fatalf("expected ErrRunNotFound on empty runID, got %v", err)
		}
	})
}

// 12. Cross-tenant cancellation (Org A requesting Org B's run) returns ErrRunNotFound (404 semantics).
func TestCancelRun_CrossTenant_ReturnsErrRunNotFound(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_tenant.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-tenant-b"
	ownerOrg := "org-B"
	attackerOrg := "org-A"

	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		OrgID:      ownerOrg,
		PipelineID: "pipe-b",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	// Cross-tenant cancellation attempt
	res, err := s.CancelRun(ctx, runID, attackerOrg)
	if res != nil {
		t.Errorf("expected nil result for cross-tenant cancel, got %+v", res)
	}
	if !errors.Is(err, store.ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got %v", err)
	}

	// Verify run status was NOT mutated
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if persisted.Status != drevtypes.StatusPending {
		t.Errorf("cross-tenant attempt altered status to %s", persisted.Status)
	}

	// Authorized cancellation succeeds
	authRes, err := s.CancelRun(ctx, runID, ownerOrg)
	if err != nil {
		t.Fatalf("authorized CancelRun() error = %v", err)
	}
	if authRes.Run.Status != drevtypes.StatusCancelled {
		t.Errorf("expected status cancelled, got %s", authRes.Run.Status)
	}
}

// 13. Concurrent cancellation requests produce consistent results.
func TestCancelRun_ConcurrentCancellations(t *testing.T) {
	t.Run("concurrent pending run cancellations", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "cancel_conc_pending.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		ctx := context.Background()
		runID := "run-conc-pending"

		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-conc-p",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun() error = %v", err)
		}

		concurrency := 20
		var wg sync.WaitGroup
		results := make([]*store.CancelResult, concurrency)
		errs := make([]error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = s.CancelRun(ctx, runID, "")
			}(i)
		}
		wg.Wait()

		var firstWins, alreadyDones int
		for i := 0; i < concurrency; i++ {
			if errs[i] != nil {
				t.Fatalf("goroutine %d failed: %v", i, errs[i])
			}
			if results[i].AlreadyDone {
				alreadyDones++
			} else {
				firstWins++
			}
		}

		if firstWins != 1 {
			t.Errorf("expected exactly 1 goroutine to win pending cancel (AlreadyDone=false), got %d", firstWins)
		}
		if alreadyDones != concurrency-1 {
			t.Errorf("expected %d goroutines to get AlreadyDone=true, got %d", concurrency-1, alreadyDones)
		}

		persisted, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun() error = %v", err)
		}
		if persisted.Status != drevtypes.StatusCancelled {
			t.Errorf("final status = %s, want cancelled", persisted.Status)
		}
	})

	t.Run("concurrent running run cancellations", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "cancel_conc_running.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		ctx := context.Background()
		runID := "run-conc-running"

		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-conc-r",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun() error = %v", err)
		}
		if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
			t.Fatalf("UpdateRunStatus() error = %v", err)
		}

		concurrency := 20
		var wg sync.WaitGroup
		results := make([]*store.CancelResult, concurrency)
		errs := make([]error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = s.CancelRun(ctx, runID, "")
			}(i)
		}
		wg.Wait()

		var firstWins, alreadyPendings int
		for i := 0; i < concurrency; i++ {
			if errs[i] != nil {
				t.Fatalf("goroutine %d failed: %v", i, errs[i])
			}
			if results[i].AlreadyPending {
				alreadyPendings++
			} else {
				firstWins++
			}
		}

		if firstWins != 1 {
			t.Errorf("expected exactly 1 goroutine to record intent (AlreadyPending=false), got %d", firstWins)
		}
		if alreadyPendings != concurrency-1 {
			t.Errorf("expected %d goroutines to observe AlreadyPending=true, got %d", concurrency-1, alreadyPendings)
		}

		persisted, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun() error = %v", err)
		}
		if persisted.Status != drevtypes.StatusRunning {
			t.Errorf("run status changed from running to %s", persisted.Status)
		}
		if persisted.CancellationRequestedAt == nil {
			t.Fatalf("expected cancellation_requested_at to be set")
		}
	})
}

// 14. Populated cancellation timestamps are preserved on repeated requests.
func TestCancelRun_PreservesPopulatedCancellationTimestamp(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_preserve_ts.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-ts-preserve"

	knownTime := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:                      runID,
		PipelineID:              "pipe-ts",
		Status:                  drevtypes.StatusRunning,
		StartedAt:               knownTime.Add(-time.Hour),
		CancellationRequestedAt: &knownTime,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	res, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if !res.AlreadyPending {
		t.Errorf("expected AlreadyPending = true")
	}
	if res.Run.CancellationRequestedAt == nil || !res.Run.CancellationRequestedAt.Equal(knownTime) {
		t.Errorf("timestamp changed: got %v, want %v", res.Run.CancellationRequestedAt, knownTime)
	}
}

// 15. Transaction rollback: mid-transaction failure rolls back all earlier mutations.
func TestCancelRun_TransactionRollback_LeavesNoPartialState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_rollback.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-rollback-trigger"
	deliveryID := "del-rollback-1"
	orgID := "org-rb"

	// 1. Create a pending run with a pending job, pipeline outbox item, and webhook delivery record
	delivery := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "acme/rollback-repo",
		RunID:      runID,
	}
	run := &drevtypes.Run{
		ID:         runID,
		OrgID:      orgID,
		PipelineID: "p-rollback",
		Status:     drevtypes.StatusPending,
	}
	jobs := []*drevtypes.RunJob{
		{ID: "j-rb-1", RunID: runID, JobName: "build", Status: drevtypes.StatusPending},
	}

	if err := s.CreateRunWithDelivery(ctx, delivery, run, jobs, "pipeline: spec-rollback"); err != nil {
		t.Fatalf("CreateRunWithDelivery() failed: %v", err)
	}

	// 2. Open a separate connection to install an intentional SQLite failure trigger on run_jobs.
	// In CancelRun(StatusPending), the statements execute in strict order:
	//   1. UPDATE runs (sets status='cancelled', failure_reason, finished_at, cancellation_requested_at)
	//   2. UPDATE run_jobs (updates pending jobs to 'cancelled')
	//   3. DELETE FROM pipeline_outbox
	// By failing BEFORE UPDATE ON run_jobs, step 1 has already mutated runs in tx,
	// but step 2 fails, proving that step 1 is rolled back completely.
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("opening rawDB for trigger setup: %v", err)
	}
	defer rawDB.Close()

	triggerSQL := `
	CREATE TRIGGER fail_cancel_jobs
	BEFORE UPDATE ON run_jobs
	BEGIN
		SELECT RAISE(ABORT, 'simulated job update failure in cancel transaction');
	END;`
	if _, err := rawDB.Exec(triggerSQL); err != nil {
		t.Fatalf("creating trigger failed: %v", err)
	}

	// 3. Invoke CancelRun — must fail on step 2 (updating run_jobs)
	res, err := s.CancelRun(ctx, runID, orgID)
	if err == nil {
		t.Fatalf("expected CancelRun to fail due to trigger, got success with res=%+v", res)
	}
	if !strings.Contains(err.Error(), "simulated job update failure") {
		t.Errorf("expected error to contain trigger message, got: %v", err)
	}

	// 4. Verify that step 1 (UPDATE runs) was ROLLED BACK completely
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() failed: %v", err)
	}
	if persisted.Status != drevtypes.StatusPending {
		t.Errorf("run status was not rolled back; got %s, want pending", persisted.Status)
	}
	if persisted.FailureReason != "" {
		t.Errorf("run failure_reason was not rolled back; got %q, want empty", persisted.FailureReason)
	}
	if persisted.InterruptedStage != "" {
		t.Errorf("run interrupted_stage was not rolled back; got %q, want empty", persisted.InterruptedStage)
	}
	if !persisted.FinishedAt.IsZero() {
		t.Errorf("run finished_at was not rolled back; got %v, want zero", persisted.FinishedAt)
	}
	if persisted.CancellationRequestedAt != nil {
		t.Errorf("run cancellation_requested_at was not rolled back; got %v, want nil", persisted.CancellationRequestedAt)
	}

	// 5. Verify that jobs remain completely pending and unmutated
	if len(persisted.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(persisted.Jobs))
	}
	job := persisted.Jobs[0]
	if job.Status != drevtypes.StatusPending {
		t.Errorf("job status was not rolled back; got %s, want pending", job.Status)
	}
	if job.FailureReason != "" {
		t.Errorf("job failure_reason was not rolled back; got %q, want empty", job.FailureReason)
	}
	if !job.FinishedAt.IsZero() {
		t.Errorf("job finished_at was not rolled back; got %v, want zero", job.FinishedAt)
	}

	// 6. Verify that step 3 was never reached and pipeline_outbox entry still exists
	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox() failed: %v", err)
	}
	foundOutbox := false
	for _, item := range outboxItems {
		if item.RunID == runID {
			foundOutbox = true
			break
		}
	}
	if !foundOutbox {
		t.Errorf("pipeline outbox entry for run %s was unexpectedly deleted or rolled back", runID)
	}

	// 7. Verify that webhook_deliveries record remains intact
	del, err := s.GetDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatalf("GetDelivery() failed: %v", err)
	}
	if del == nil || del.DeliveryID != deliveryID {
		t.Errorf("webhook delivery was deleted or corrupted")
	}

	// 8. Clean up: Drop the trigger and verify that CancelRun now succeeds cleanly
	if _, err := rawDB.Exec(`DROP TRIGGER IF EXISTS fail_cancel_jobs`); err != nil {
		t.Fatalf("dropping trigger failed: %v", err)
	}

	successRes, err := s.CancelRun(ctx, runID, orgID)
	if err != nil {
		t.Fatalf("CancelRun failed after dropping trigger: %v", err)
	}
	if successRes.Run.Status != drevtypes.StatusCancelled {
		t.Errorf("run did not transition to cancelled after trigger drop: got %s", successRes.Run.Status)
	}

	// 9. Subtest: pre-canceled context fails cleanly without mutation
	t.Run("pre-canceled context fails without mutation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		res, err := s.CancelRun(cancelCtx, runID, "")
		if err == nil {
			t.Fatalf("expected error on pre-canceled context, got res=%+v", res)
		}
	})
}

// ─── Step 5.1: Store-Level Cancellation vs Success Linearization Tests ───────

func TestUpdateRunStatus_PendingCannotBecomeSuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pending_success_rejected.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-pending-success"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	// Directly transitioning from pending to success must be rejected
	err = s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess)
	if err == nil {
		t.Fatal("expected error transitioning pending -> success, got nil")
	}
	if errors.Is(err, store.ErrCancellationRequested) {
		t.Errorf("rejected pending run must not be classified as ErrCancellationRequested")
	}
	if !errors.Is(err, store.ErrRunAlreadyTerminal) {
		t.Errorf("expected ErrRunAlreadyTerminal, got: %v", err)
	}

	// Status must remain pending
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if persisted.Status != drevtypes.StatusPending {
		t.Errorf("status was mutated to %s, want %s", persisted.Status, drevtypes.StatusPending)
	}
	if !persisted.FinishedAt.IsZero() {
		t.Errorf("finished_at was unexpectedly set: %v", persisted.FinishedAt)
	}
}

func TestUpdateRunStatus_AlreadySuccessfulIdempotency(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotent_success.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-idemp-success"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("UpdateRunStatus(running) error = %v", err)
	}
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("UpdateRunStatus(success) error = %v", err)
	}

	initial, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	initialFinishedAt := initial.FinishedAt
	if initialFinishedAt.IsZero() {
		t.Fatal("expected finished_at to be populated")
	}

	time.Sleep(10 * time.Millisecond)

	// Idempotent update to success must succeed
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("idempotent UpdateRunStatus(success) failed: %v", err)
	}

	after, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if after.Status != drevtypes.StatusSuccess {
		t.Errorf("status mutated to %s", after.Status)
	}
	if !after.FinishedAt.Equal(initialFinishedAt) {
		t.Errorf("finished_at changed on idempotent update: before %v, after %v", initialFinishedAt, after.FinishedAt)
	}
}

func TestUpdateRunStatus_SuccessAllowed_WhenNoCancellationIntent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "success_no_cancel.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-success-allowed"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("UpdateRunStatus(running) error = %v", err)
	}

	// Finalizing as success must succeed when cancellation_requested_at is NULL
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("UpdateRunStatus(success) error = %v", err)
	}

	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if run.Status != drevtypes.StatusSuccess {
		t.Errorf("status = %s, want %s", run.Status, drevtypes.StatusSuccess)
	}
	if run.FinishedAt.IsZero() {
		t.Errorf("finished_at was not populated")
	}
	if run.CancellationRequestedAt != nil {
		t.Errorf("cancellation_requested_at was unexpectedly populated: %v", run.CancellationRequestedAt)
	}
}

func TestUpdateRunStatus_SuccessRejected_WhenCancellationRequested(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "success_rejected_cancel.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-success-rejected"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         runID,
		PipelineID: "p-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("UpdateRunStatus(running) error = %v", err)
	}

	// 1. Persist cancellation intent
	cancelRes, err := s.CancelRun(ctx, runID, "")
	if err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if !cancelRes.WasRunning || cancelRes.Run.CancellationRequestedAt == nil {
		t.Fatalf("expected running run with cancellation_requested_at set, got %+v", cancelRes)
	}
	originalCancelTime := *cancelRes.Run.CancellationRequestedAt

	// 2. Attempt to finalize as success: MUST be rejected with ErrCancellationRequested
	err = s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess)
	if err == nil {
		t.Fatal("expected UpdateRunStatus(success) to fail due to cancellation intent, got nil")
	}
	if !errors.Is(err, store.ErrCancellationRequested) {
		t.Fatalf("expected ErrCancellationRequested, got: %v", err)
	}

	// 3. Verify database state remains running with cancellation_requested_at intact
	persisted, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if persisted.Status != drevtypes.StatusRunning {
		t.Errorf("status was mutated to %s, want %s", persisted.Status, drevtypes.StatusRunning)
	}
	if persisted.CancellationRequestedAt == nil {
		t.Fatal("cancellation_requested_at was cleared")
	}
	if !persisted.CancellationRequestedAt.Equal(originalCancelTime) {
		t.Errorf("cancellation_requested_at changed: was %v, now %v", originalCancelTime, *persisted.CancellationRequestedAt)
	}
	if !persisted.FinishedAt.IsZero() {
		t.Errorf("finished_at was unexpectedly set: %v", persisted.FinishedAt)
	}
}

func TestUpdateRunStatus_PrecedingFailureAndCancellationFinalizationAllowed_WhenCancellationRequested(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "failure_cancel_allowed.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Subtest 1: Preceding genuine failure can finalize despite cancellation intent
	t.Run("preceding failure allowed", func(t *testing.T) {
		runID := "run-preceding-fail"
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-fail",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		_ = s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning)
		_, err := s.CancelRun(ctx, runID, "")
		if err != nil {
			t.Fatalf("CancelRun: %v", err)
		}

		// Updating to StatusFailed with diagnostics must succeed
		if err := s.UpdateRunStatusWithDiagnostics(ctx, runID, drevtypes.StatusFailed, drevtypes.FailureReasonJobFailed, drevtypes.StageExecution); err != nil {
			t.Fatalf("UpdateRunStatusWithDiagnostics(failed) error = %v", err)
		}

		run, _ := s.GetRun(ctx, runID)
		if run.Status != drevtypes.StatusFailed {
			t.Errorf("status = %s, want %s", run.Status, drevtypes.StatusFailed)
		}
		if run.FailureReason != drevtypes.FailureReasonJobFailed {
			t.Errorf("failure_reason = %s, want %s", run.FailureReason, drevtypes.FailureReasonJobFailed)
		}
	})

	// Subtest 2: Cancellation finalization can finalize when cancellation intent was requested
	t.Run("cancellation finalization allowed", func(t *testing.T) {
		runID := "run-cancel-finalize"
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-cancel",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		_ = s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning)
		_, err := s.CancelRun(ctx, runID, "")
		if err != nil {
			t.Fatalf("CancelRun: %v", err)
		}

		// Updating to StatusCancelled with diagnostics must succeed
		if err := s.UpdateRunStatusWithDiagnostics(ctx, runID, drevtypes.StatusCancelled, string(drevtypes.StatusCancelled), drevtypes.StageExecution); err != nil {
			t.Fatalf("UpdateRunStatusWithDiagnostics(cancelled) error = %v", err)
		}

		run, _ := s.GetRun(ctx, runID)
		if run.Status != drevtypes.StatusCancelled {
			t.Errorf("status = %s, want %s", run.Status, drevtypes.StatusCancelled)
		}
		if run.FailureReason != string(drevtypes.StatusCancelled) {
			t.Errorf("failure_reason = %s, want %s", run.FailureReason, drevtypes.StatusCancelled)
		}
	})
}

func TestUpdateRunStatus_TerminalRunReturnsErrRunAlreadyTerminal_NotErrCancellationRequested(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "terminal_run_err.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Run already failed
	runFailed := "run-already-failed"
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runFailed, PipelineID: "p", Status: drevtypes.StatusPending})
	_ = s.UpdateRunStatus(ctx, runFailed, drevtypes.StatusFailed)

	err = s.UpdateRunStatus(ctx, runFailed, drevtypes.StatusSuccess)
	if !errors.Is(err, store.ErrRunAlreadyTerminal) {
		t.Errorf("expected ErrRunAlreadyTerminal on failed run, got: %v", err)
	}
	if errors.Is(err, store.ErrCancellationRequested) {
		t.Errorf("unexpected ErrCancellationRequested on failed run")
	}

	// 2. Run already cancelled
	runCancelled := "run-already-cancelled"
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runCancelled, PipelineID: "p", Status: drevtypes.StatusPending})
	_ = s.UpdateRunStatus(ctx, runCancelled, drevtypes.StatusCancelled)

	err = s.UpdateRunStatus(ctx, runCancelled, drevtypes.StatusSuccess)
	if !errors.Is(err, store.ErrRunAlreadyTerminal) {
		t.Errorf("expected ErrRunAlreadyTerminal on cancelled run, got: %v", err)
	}
	if errors.Is(err, store.ErrCancellationRequested) {
		t.Errorf("unexpected ErrCancellationRequested on cancelled run")
	}
}

func TestUpdateRunStatus_MissingRunReturnsErrNoRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing_run.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	err = s.UpdateRunStatus(context.Background(), "non-existent-run-id", drevtypes.StatusSuccess)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows for missing run, got: %v", err)
	}
}

func TestUpdateRunStatus_ConcurrentCancelAndSuccess_Linearization(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent_linearization.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	const iterations = 10

	for i := 0; i < iterations; i++ {
		runID := fmt.Sprintf("run-concurrent-lin-%d", i)
		if err := s.CreateRun(ctx, &drevtypes.Run{
			ID:         runID,
			PipelineID: "p-conc",
			Status:     drevtypes.StatusPending,
		}); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		if err := s.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
			t.Fatalf("UpdateRunStatus: %v", err)
		}

		var startWg sync.WaitGroup
		startWg.Add(1)
		var doneWg sync.WaitGroup
		doneWg.Add(2)

		var cancelRes *store.CancelResult
		var cancelErr error
		var successErr error

		go func() {
			defer doneWg.Done()
			startWg.Wait()
			cancelRes, cancelErr = s.CancelRun(ctx, runID, "")
		}()

		go func() {
			defer doneWg.Done()
			startWg.Wait()
			successErr = s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess)
		}()

		startWg.Done()
		doneWg.Wait()

		// Winner verification:
		// Case A: CancelRun committed first -> successErr MUST be ErrCancellationRequested, run remains running with cancel intent
		// Case B: Success update committed first -> cancelErr MUST be ErrRunAlreadyTerminal, run is StatusSuccess
		run, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}

		if run.Status == drevtypes.StatusSuccess {
			// Success won
			if successErr != nil {
				t.Errorf("iter %d: run is success but successErr was %v", i, successErr)
			}
			if cancelErr == nil || !errors.Is(cancelErr, store.ErrRunAlreadyTerminal) {
				t.Errorf("iter %d: success won but cancelErr was %v (want ErrRunAlreadyTerminal)", i, cancelErr)
			}
			if run.CancellationRequestedAt != nil {
				t.Errorf("iter %d: run is success but cancellation_requested_at is set: %v", i, run.CancellationRequestedAt)
			}
		} else if run.Status == drevtypes.StatusRunning {
			// Cancel intent won
			if cancelErr != nil {
				t.Errorf("iter %d: run is running (cancel won) but cancelErr was %v", i, cancelErr)
			}
			if cancelRes == nil || cancelRes.Run.CancellationRequestedAt == nil {
				t.Errorf("iter %d: cancel won but cancellation_requested_at not populated", i)
			}
			if successErr == nil || !errors.Is(successErr, store.ErrCancellationRequested) {
				t.Errorf("iter %d: cancel won but successErr was %v (want ErrCancellationRequested)", i, successErr)
			}
		} else {
			t.Errorf("iter %d: unexpected final status: %s", i, run.Status)
		}
	}
}
