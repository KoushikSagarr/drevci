//go:build postgres

package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func testDB(t *testing.T) *PostgresStore {
	t.Helper()
	connStr := os.Getenv("DREV_DB_URL")
	if connStr == "" {
		t.Skip("DREV_DB_URL not set — skipping postgres integration tests")
	}

	s, err := OpenPostgres(connStr)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Isolated schema for this test run
	schema := fmt.Sprintf("test_%d", rand.New(rand.NewSource(time.Now().UnixNano())).Int63())
	_, err = s.db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schema))
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	_, err = s.db.Exec(fmt.Sprintf("SET search_path TO %q, public", schema))
	if err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	t.Cleanup(func() {
		s.db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema))
	})

	return s
}

func TestPostgresStore_CreateRun(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:          "run-pg-001",
		OrgID:       "org-test-001",
		PipelineID:  "my-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
}

func TestPostgresStore_GetRun(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:          "run-pg-002",
		OrgID:       "org-test-001",
		PipelineID:  "my-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.ID != run.ID {
		t.Errorf("got ID %q, want %q", got.ID, run.ID)
	}
	if got.Status != drevtypes.StatusPending {
		t.Errorf("got status %q, want pending", got.Status)
	}
}

func TestPostgresStore_UpdateRunStatus(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:          "run-pg-003",
		OrgID:       "org-test-001",
		PipelineID:  "my-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.UpdateRunStatus(ctx, run.ID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("UpdateRunStatus: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Status != drevtypes.StatusSuccess {
		t.Errorf("got status %q, want success", got.Status)
	}
	if got.FinishedAt.IsZero() {
		t.Error("expected FinishedAt to be set")
	}
}

func TestPostgresStore_ListRuns(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	for i := range 3 {
		run := &drevtypes.Run{
			ID:          fmt.Sprintf("run-list-%d", i),
			OrgID:       "org-test-list",
			PipelineID:  "pipeline",
			Status:      drevtypes.StatusSuccess,
			TriggeredBy: "api",
		}
		if err := s.CreateRun(ctx, run); err != nil {
			t.Fatalf("CreateRun %d: %v", i, err)
		}
	}

	runs, err := s.ListRunsByOrg(ctx, "org-test-list", 10)
	if err != nil {
		t.Fatalf("ListRunsByOrg: %v", err)
	}
	if len(runs) != 3 {
		t.Errorf("got %d runs, want 3", len(runs))
	}
}

func TestPostgresStore_CreateOrg(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	org := &drevtypes.Org{
		Name:             "Acme Corp",
		Slug:             "acme",
		Plan:             "team",
		WorkerLimit:      3,
		QueueLimit:       50,
		LogRetentionDays: 30,
	}

	if err := s.CreateOrg(ctx, org); err != nil {
		t.Fatalf("CreateOrg: %v", err)
	}
	if org.ID == "" {
		t.Error("expected org.ID to be set after create")
	}

	got, err := s.GetOrgBySlug(ctx, "acme")
	if err != nil {
		t.Fatalf("GetOrgBySlug: %v", err)
	}
	if got.Name != "Acme Corp" {
		t.Errorf("got name %q, want Acme Corp", got.Name)
	}
	if got.WorkerLimit != 3 {
		t.Errorf("got worker_limit %d, want 3", got.WorkerLimit)
	}
}

func TestPostgresStore_ValidateToken(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	org := &drevtypes.Org{
		Name: "Token Test Org", Slug: "token-test-org",
		Plan: "free", WorkerLimit: 1, QueueLimit: 10, LogRetentionDays: 7,
	}
	if err := s.CreateOrg(ctx, org); err != nil {
		t.Fatalf("CreateOrg: %v", err)
	}

	token := &drevtypes.APIToken{
		OrgID:     org.ID,
		Name:      "CI Token",
		TokenHash: "sha256-test-hash-abc123",
		CreatedBy: "user-001",
	}
	if err := s.CreateToken(ctx, token); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	validated, err := s.ValidateToken(ctx, "sha256-test-hash-abc123")
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if validated.OrgID != org.ID {
		t.Errorf("got org_id %q, want %q", validated.OrgID, org.ID)
	}
}

func TestPostgresStore_RecordUsage(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	org := &drevtypes.Org{
		Name: "Usage Test Org", Slug: "usage-test-org",
		Plan: "free", WorkerLimit: 1, QueueLimit: 10, LogRetentionDays: 7,
	}
	if err := s.CreateOrg(ctx, org); err != nil {
		t.Fatalf("CreateOrg: %v", err)
	}

	run := &drevtypes.Run{
		ID: "run-usage-001", OrgID: org.ID,
		PipelineID: "test-pipeline", Status: drevtypes.StatusSuccess,
		TriggeredBy: "api",
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.RecordUsage(ctx, org.ID, "pipeline_run", run.ID); err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}

	count, err := s.GetMonthlyUsage(ctx, org.ID)
	if err != nil {
		t.Fatalf("GetMonthlyUsage: %v", err)
	}
	if count != 1 {
		t.Errorf("got monthly usage %d, want 1", count)
	}
}

func TestPostgresStore_WebhookDeliveryIdempotency(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Nil or empty delivery returns error
	if err := s.RecordDelivery(ctx, nil); err == nil {
		t.Errorf("expected error when recording nil delivery, got nil")
	}
	if err := s.RecordDelivery(ctx, &drevtypes.WebhookDelivery{}); err == nil {
		t.Errorf("expected error when recording empty delivery_id, got nil")
	}

	// 2. Querying non-existent delivery returns nil, nil
	got, err := s.GetDelivery(ctx, "nonexistent-delivery-id")
	if err != nil {
		t.Fatalf("GetDelivery(nonexistent) error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for nonexistent delivery, got %+v", got)
	}

	// 3. Successfully record a delivery
	delivery := &drevtypes.WebhookDelivery{
		DeliveryID: "delivery-pg-guid-12345",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   42,
		RunID:      "run-pg-guid-67890",
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}
	if err := s.RecordDelivery(ctx, delivery); err != nil {
		t.Fatalf("RecordDelivery() error = %v", err)
	}

	// 4. Retrieve the recorded delivery and assert fields
	rec, err := s.GetDelivery(ctx, delivery.DeliveryID)
	if err != nil {
		t.Fatalf("GetDelivery() error = %v", err)
	}
	if rec == nil {
		t.Fatalf("expected recorded delivery, got nil")
	}
	if rec.DeliveryID != delivery.DeliveryID {
		t.Errorf("got DeliveryID %q, want %q", rec.DeliveryID, delivery.DeliveryID)
	}
	if rec.EventType != delivery.EventType {
		t.Errorf("got EventType %q, want %q", rec.EventType, delivery.EventType)
	}
	if rec.Repo != delivery.Repo {
		t.Errorf("got Repo %q, want %q", rec.Repo, delivery.Repo)
	}
	if rec.PRNumber != delivery.PRNumber {
		t.Errorf("got PRNumber %d, want %d", rec.PRNumber, delivery.PRNumber)
	}
	if rec.RunID != delivery.RunID {
		t.Errorf("got RunID %q, want %q", rec.RunID, delivery.RunID)
	}

	// 5. Duplicate delivery returns ErrDuplicateDelivery
	dupDelivery := &drevtypes.WebhookDelivery{
		DeliveryID: delivery.DeliveryID,
		EventType:  "pull_request",
		Repo:       delivery.Repo,
		PRNumber:   delivery.PRNumber,
		RunID:      "run-different-id",
	}
	err = s.RecordDelivery(ctx, dupDelivery)
	if err == nil {
		t.Fatalf("expected ErrDuplicateDelivery on duplicate, got nil")
	}
	if !errors.Is(err, ErrDuplicateDelivery) && !strings.Contains(err.Error(), "duplicate webhook delivery") {
		t.Errorf("expected duplicate error, got: %v", err)
	}
}

func TestPostgresStore_CreateRunWithDelivery_Atomicity(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Successful atomic commit of delivery, run, and jobs
	del1 := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-atomic-001",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   10,
		RunID:      "run-pg-atomic-001",
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}
	run1 := &drevtypes.Run{
		ID:          del1.RunID,
		PipelineID:  "pipe-1",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
		CommitSHA:   "abc1234567",
		Branch:      "main",
	}
	jobs1 := []*drevtypes.RunJob{
		{ID: "job-pg-atomic-1", RunID: run1.ID, JobName: "build", Status: drevtypes.StatusPending},
		{ID: "job-pg-atomic-2", RunID: run1.ID, JobName: "test", Status: drevtypes.StatusPending},
	}

	if err := s.CreateRunWithDelivery(ctx, del1, run1, jobs1, `{"name":"test-spec-1"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(1) error = %v", err)
	}

	// Verify all 3 entities exist
	gotDel, err := s.GetDelivery(ctx, del1.DeliveryID)
	if err != nil || gotDel == nil {
		t.Fatalf("expected delivery in store, got %+v (err: %v)", gotDel, err)
	}
	gotRun, err := s.GetRun(ctx, run1.ID)
	if err != nil || gotRun == nil {
		t.Fatalf("expected run in store, got %+v (err: %v)", gotRun, err)
	}
	gotJobs, err := s.GetRunJobs(ctx, run1.ID)
	if err != nil || len(gotJobs) != 2 {
		t.Fatalf("expected 2 jobs in store, got %d (err: %v)", len(gotJobs), err)
	}
	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(outboxItems) != 1 || outboxItems[0].RunID != run1.ID {
		t.Fatalf("expected 1 outbox item for run1, got %+v (err: %v)", outboxItems, err)
	}

	// 2. Rollback when later job insertion fails (duplicate job ID triggers constraint error)
	del2 := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-atomic-002",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   11,
		RunID:      "run-pg-atomic-002",
	}
	run2 := &drevtypes.Run{
		ID:          del2.RunID,
		PipelineID:  "pipe-2",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}
	failingJobs := []*drevtypes.RunJob{
		{ID: "job-pg-atomic-fail-1", RunID: run2.ID, JobName: "step-1", Status: drevtypes.StatusPending},
		{ID: "job-pg-atomic-fail-1", RunID: run2.ID, JobName: "step-2", Status: drevtypes.StatusPending},
	}

	err = s.CreateRunWithDelivery(ctx, del2, run2, failingJobs, `{"name":"test-spec-fail"}`)
	if err == nil {
		t.Fatalf("expected error on duplicate job ID, got nil")
	}

	// PROVE ROLLBACK: Neither delivery, run, nor partial job must exist in database!
	delCheck, err := s.GetDelivery(ctx, del2.DeliveryID)
	if err != nil || delCheck != nil {
		t.Fatalf("expected delivery2 to be rolled back, found: %+v", delCheck)
	}
	runCheck, err := s.GetRun(ctx, run2.ID)
	if err == nil && runCheck != nil {
		t.Fatalf("expected run2 to be rolled back, found: %+v", runCheck)
	}
	jobsCheck, err := s.GetRunJobs(ctx, run2.ID)
	if len(jobsCheck) != 0 {
		t.Fatalf("expected 0 jobs for run2, found %d", len(jobsCheck))
	}
	outboxCheck, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(outboxCheck) != 1 || outboxCheck[0].RunID != run1.ID {
		t.Fatalf("outbox should only contain run1, found: %+v", outboxCheck)
	}

	// 3. Same delivery ID can be retried after rolled-back transaction
	validJobs2 := []*drevtypes.RunJob{
		{ID: "job-pg-atomic-valid-1", RunID: run2.ID, JobName: "step-1", Status: drevtypes.StatusPending},
		{ID: "job-pg-atomic-valid-2", RunID: run2.ID, JobName: "step-2", Status: drevtypes.StatusPending},
	}
	if err := s.CreateRunWithDelivery(ctx, del2, run2, validJobs2, `{"name":"test-spec-2"}`); err != nil {
		t.Fatalf("retrying CreateRunWithDelivery with same delivery ID failed: %v", err)
	}
	retriedDel, _ := s.GetDelivery(ctx, del2.DeliveryID)
	if retriedDel == nil || retriedDel.RunID != run2.ID {
		t.Errorf("retried delivery not found or mismatched: %+v", retriedDel)
	}

	// 4. Duplicate delivery returns ErrDuplicateDelivery
	delDup := &drevtypes.WebhookDelivery{
		DeliveryID: del2.DeliveryID,
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      "run-pg-atomic-ghost",
	}
	runDup := &drevtypes.Run{
		ID:         "run-pg-atomic-ghost",
		PipelineID: "pipe-ghost",
		Status:     drevtypes.StatusPending,
	}
	err = s.CreateRunWithDelivery(ctx, delDup, runDup, nil, "{}")
	if err == nil {
		t.Fatalf("expected ErrDuplicateDelivery on duplicate delivery ID, got nil")
	}
	if !errors.Is(err, ErrDuplicateDelivery) {
		t.Errorf("expected ErrDuplicateDelivery, got: %v", err)
	}

	// 5. DeleteDelivery releases delivery claim allowing re-creation
	if err := s.DeleteDelivery(ctx, del2.DeliveryID); err != nil {
		t.Fatalf("DeleteDelivery failed: %v", err)
	}
	delDeleted, _ := s.GetDelivery(ctx, del2.DeliveryID)
	if delDeleted != nil {
		t.Errorf("expected delivery to be deleted, got: %+v", delDeleted)
	}
}

func TestPostgresStore_ResetGhostRuns_ReleasesWebhookDeliveries(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Seed interrupted runs (pending, running) and terminal run (success)
	runPending := &drevtypes.Run{
		ID:         "run-pg-ghost-pending",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delPending := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-ghost-pending",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runPending.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delPending, runPending, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(pending): %v", err)
	}

	runRunning := &drevtypes.Run{
		ID:         "run-pg-ghost-running",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusRunning,
	}
	delRunning := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-ghost-running",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runRunning.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delRunning, runRunning, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(running): %v", err)
	}

	runSuccess := &drevtypes.Run{
		ID:         "run-pg-ghost-success",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusSuccess,
	}
	delSuccess := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-ghost-success",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runSuccess.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delSuccess, runSuccess, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(success): %v", err)
	}

	// 2. Call ResetGhostRuns
	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns failed: %v", err)
	}
	if len(interrupted) != 2 {
		t.Fatalf("expected 2 interrupted runs, got %d: %v", len(interrupted), interrupted)
	}

	// 3. Verify interrupted runs are now failed
	r1, _ := s.GetRun(ctx, runPending.ID)
	if r1.Status != drevtypes.StatusFailed || r1.FinishedAt.IsZero() {
		t.Errorf("expected runPending to be failed with finished_at, got: %+v", r1)
	}
	r2, _ := s.GetRun(ctx, runRunning.ID)
	if r2.Status != drevtypes.StatusFailed || r2.FinishedAt.IsZero() {
		t.Errorf("expected runRunning to be failed with finished_at, got: %+v", r2)
	}

	// 4. Verify webhook delivery records for interrupted runs are RELEASED
	dPending, err := s.GetDelivery(ctx, delPending.DeliveryID)
	if err != nil || dPending != nil {
		t.Errorf("expected del-pg-ghost-pending to be released, got: %+v", dPending)
	}
	dRunning, err := s.GetDelivery(ctx, delRunning.DeliveryID)
	if err != nil || dRunning != nil {
		t.Errorf("expected del-pg-ghost-running to be released, got: %+v", dRunning)
	}

	// 5. Verify webhook delivery records for SUCCESSFUL runs are NOT touched
	dSuccess, err := s.GetDelivery(ctx, delSuccess.DeliveryID)
	if err != nil || dSuccess == nil {
		t.Fatalf("expected del-pg-ghost-success to be preserved, got nil (err: %v)", err)
	}
	if dSuccess.RunID != runSuccess.ID {
		t.Errorf("expected delSuccess.RunID == %q, got %q", runSuccess.ID, dSuccess.RunID)
	}

	// 6. Proves retry with same delivery ID succeeds after recovery
	runRetried := &drevtypes.Run{
		ID:         "run-pg-retried-after-recovery",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delRetried := &drevtypes.WebhookDelivery{
		DeliveryID: delPending.DeliveryID, // Reusing interrupted delivery ID
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runRetried.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delRetried, runRetried, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery after recovery failed: %v", err)
	}
	dFinal, _ := s.GetDelivery(ctx, delPending.DeliveryID)
	if dFinal == nil || dFinal.RunID != runRetried.ID {
		t.Errorf("retried delivery not pointing to new run: %+v", dFinal)
	}
}

func TestPostgresStore_ClaimRunForExecution_Success(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-pg-claim-001",
		PipelineID: "pipe-claim",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-claim-001",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	spec := `{"name":"test-spec","jobs":[{"name":"j1"}]}`

	if err := s.CreateRunWithDelivery(ctx, del, run, nil, spec); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	items, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(items) != 1 || items[0].RunID != run.ID {
		t.Fatalf("expected 1 pending outbox item, got: %+v (err: %v)", items, err)
	}

	if err := s.ClaimRunForExecution(ctx, run.ID); err != nil {
		t.Fatalf("ClaimRunForExecution failed: %v", err)
	}

	gotRun, err := s.GetRun(ctx, run.ID)
	if err != nil || gotRun == nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if gotRun.Status != drevtypes.StatusRunning {
		t.Errorf("expected status running, got %q", gotRun.Status)
	}
	if gotRun.StartedAt.IsZero() {
		t.Errorf("expected non-zero started_at after claim")
	}

	itemsAfter, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(itemsAfter) != 0 {
		t.Fatalf("expected outbox to be empty after claim, got: %+v", itemsAfter)
	}
}

func TestPostgresStore_ClaimRunForExecution_Concurrency(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-pg-concurrent-claim",
		PipelineID: "pipe-claim",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-concurrent-claim",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, nil, `{"name":"concurrent"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	const concurrency = 10
	var wg sync.WaitGroup
	var successCount int64
	var alreadyClaimedCount int64
	var unexpectedErrors []error
	var mu sync.Mutex

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			err := s.ClaimRunForExecution(ctx, run.ID)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else if errors.Is(err, ErrRunAlreadyClaimed) {
				atomic.AddInt64(&alreadyClaimedCount, 1)
			} else {
				mu.Lock()
				unexpectedErrors = append(unexpectedErrors, err)
				mu.Unlock()
			}
		}()
	}

	close(startGate)
	wg.Wait()

	if len(unexpectedErrors) > 0 {
		t.Fatalf("encountered unexpected claim errors: %v", unexpectedErrors)
	}
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful claim, got %d", successCount)
	}
	if alreadyClaimedCount != concurrency-1 {
		t.Errorf("expected %d ErrRunAlreadyClaimed, got %d", concurrency-1, alreadyClaimedCount)
	}
}

func TestPostgresStore_ClaimRunForExecution_Rejection(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	err := s.ClaimRunForExecution(ctx, "non-existent-run-id")
	if !errors.Is(err, ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for missing run, got: %v", err)
	}

	runRunning := &drevtypes.Run{
		ID:         "run-pg-already-running",
		PipelineID: "p1",
		Status:     drevtypes.StatusRunning,
	}
	if err := s.CreateRun(ctx, runRunning); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}
	err = s.ClaimRunForExecution(ctx, runRunning.ID)
	if !errors.Is(err, ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for running run, got: %v", err)
	}
}

func TestPostgresStore_GetPendingOutbox(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	t1 := time.Now().UTC().Add(-30 * time.Minute)
	t2 := time.Now().UTC().Add(-20 * time.Minute)

	del1 := &drevtypes.WebhookDelivery{DeliveryID: "pg-d1", RunID: "pg-r1", CreatedAt: t1}
	del2 := &drevtypes.WebhookDelivery{DeliveryID: "pg-d2", RunID: "pg-r2", CreatedAt: t2}

	run1 := &drevtypes.Run{ID: "pg-r1", PipelineID: "p", Status: drevtypes.StatusPending}
	run2 := &drevtypes.Run{ID: "pg-r2", PipelineID: "p", Status: drevtypes.StatusPending}

	if err := s.CreateRunWithDelivery(ctx, del1, run1, nil, `{"id":"spec1"}`); err != nil {
		t.Fatalf("seed r1 failed: %v", err)
	}
	if err := s.CreateRunWithDelivery(ctx, del2, run2, nil, `{"id":"spec2"}`); err != nil {
		t.Fatalf("seed r2 failed: %v", err)
	}

	if err := s.ClaimRunForExecution(ctx, "pg-r2"); err != nil {
		t.Fatalf("claim r2 failed: %v", err)
	}

	items, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox error = %v", err)
	}
	if len(items) != 1 || items[0].RunID != "pg-r1" {
		t.Fatalf("expected [pg-r1], got: %+v", items)
	}
}

func TestPostgresStore_FailRunWithDelivery_Success(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-pg-fail-test-01",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-fail-test-01",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	jobs := []*drevtypes.RunJob{
		{ID: "pg-j1", RunID: run.ID, JobName: "build", Status: drevtypes.StatusPending},
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, `{"name":"fail-spec"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	if err := s.FailRunWithDelivery(ctx, run.ID, []string{"pg-j1"}, del.DeliveryID); err != nil {
		t.Fatalf("FailRunWithDelivery failed: %v", err)
	}

	r, err := s.GetRun(ctx, run.ID)
	if err != nil || r == nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if r.Status != drevtypes.StatusFailed || r.FinishedAt.IsZero() {
		t.Errorf("expected run failed with finished_at, got: %+v", r)
	}

	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(outboxItems) != 0 {
		t.Errorf("expected outbox entry to be deleted, found: %+v", outboxItems)
	}

	delCheck, err := s.GetDelivery(ctx, del.DeliveryID)
	if err != nil || delCheck != nil {
		t.Errorf("expected delivery claim to be deleted, found: %+v", delCheck)
	}
}

func TestPostgresStore_FailRunWithDelivery_Rollback(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-pg-rollback-test-01",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-rollback-test-01",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, nil, `{"name":"rollback-spec"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	err := s.FailRunWithDelivery(ctx, "non-existent-run-id", nil, del.DeliveryID)
	if err == nil {
		t.Fatalf("expected error on non-existent run, got nil")
	}

	delCheck, err := s.GetDelivery(ctx, del.DeliveryID)
	if err != nil || delCheck == nil {
		t.Fatalf("delivery claim must NOT be deleted when FailRunWithDelivery fails: %+v", delCheck)
	}
	if delCheck.RunID != run.ID {
		t.Errorf("expected delivery to point to original run %s, got %s", run.ID, delCheck.RunID)
	}
}

func TestPostgresStore_CreateRunWithDelivery_EmptyPipelineSpecRejected(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run1 := &drevtypes.Run{ID: "00000000-0000-0000-0000-000000000001", PipelineID: "p1", Status: drevtypes.StatusPending}
	del1 := &drevtypes.WebhookDelivery{DeliveryID: "del-pg-empty-1", RunID: run1.ID}
	err := s.CreateRunWithDelivery(ctx, del1, run1, nil, "")
	if err == nil {
		t.Fatalf("expected error for empty pipelineSpec, got nil")
	}

	dCheck, _ := s.GetDelivery(ctx, del1.DeliveryID)
	if dCheck != nil {
		t.Errorf("delivery should not be persisted on empty spec: %+v", dCheck)
	}
	rCheck, _ := s.GetRun(ctx, run1.ID)
	if rCheck != nil {
		t.Errorf("run should not be persisted on empty spec: %+v", rCheck)
	}
	outboxItems, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxItems) != 0 {
		t.Errorf("outbox should be empty, got %d items", len(outboxItems))
	}
}

func TestPostgresStore_FailRunWithDelivery_ScopedToRun(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runA := &drevtypes.Run{ID: "00000000-0000-0000-0000-00000000000a", PipelineID: "pA", Status: drevtypes.StatusPending}
	delA := &drevtypes.WebhookDelivery{DeliveryID: "del-pg-A", RunID: runA.ID}
	jobsA := []*drevtypes.RunJob{{ID: "00000000-0000-0000-0000-00000000001a", RunID: runA.ID, JobName: "build", Status: drevtypes.StatusPending}}
	if err := s.CreateRunWithDelivery(ctx, delA, runA, jobsA, `{"name":"specA"}`); err != nil {
		t.Fatalf("seed run A failed: %v", err)
	}

	runB := &drevtypes.Run{ID: "00000000-0000-0000-0000-00000000000b", PipelineID: "pB", Status: drevtypes.StatusPending}
	delB := &drevtypes.WebhookDelivery{DeliveryID: "del-pg-B", RunID: runB.ID}
	jobsB := []*drevtypes.RunJob{{ID: "00000000-0000-0000-0000-00000000001b", RunID: runB.ID, JobName: "build", Status: drevtypes.StatusPending}}
	if err := s.CreateRunWithDelivery(ctx, delB, runB, jobsB, `{"name":"specB"}`); err != nil {
		t.Fatalf("seed run B failed: %v", err)
	}

	// Case 1: Pass mismatched job ID (job B belongs to Run B, not Run A)
	err := s.FailRunWithDelivery(ctx, runA.ID, []string{jobsB[0].ID}, delA.DeliveryID)
	if err == nil {
		t.Fatalf("expected error when passing mismatched job ID, got nil")
	}
	if !strings.Contains(err.Error(), "not found for run") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify rollback: Run A still pending, Delivery A intact
	delACheck, _ := s.GetDelivery(ctx, delA.DeliveryID)
	if delACheck == nil {
		t.Fatalf("delivery A must NOT be deleted when job validation fails")
	}

	// Case 2: Pass mismatched delivery ID (del B belongs to Run B, not Run A)
	err = s.FailRunWithDelivery(ctx, runA.ID, []string{jobsA[0].ID}, delB.DeliveryID)
	if err == nil {
		t.Fatalf("expected error when passing mismatched delivery ID, got nil")
	}
	if !strings.Contains(err.Error(), "webhook delivery del-pg-B not found for run") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify Run B and Delivery B are completely untouched
	delBCheck, err := s.GetDelivery(ctx, delB.DeliveryID)
	if err != nil || delBCheck == nil {
		t.Fatalf("delivery B must not be deleted: %+v", delBCheck)
	}
	jobsBCheck, _ := s.GetRunJobs(ctx, runB.ID)
	if len(jobsBCheck) != 1 || jobsBCheck[0].Status != drevtypes.StatusPending {
		t.Errorf("job B should still be pending, got: %+v", jobsBCheck[0])
	}

	// Case 3: Correctly scoped cleanup for Run A
	if err := s.FailRunWithDelivery(ctx, runA.ID, []string{jobsA[0].ID}, delA.DeliveryID); err != nil {
		t.Fatalf("valid FailRunWithDelivery failed: %v", err)
	}
	delACheck, _ = s.GetDelivery(ctx, delA.DeliveryID)
	if delACheck != nil {
		t.Errorf("delivery A should be deleted after successful FailRunWithDelivery")
	}
}

func TestPostgres_MigrationFileExistsAndValid(t *testing.T) {
	data, err := os.ReadFile("../../migrations/004_fix_uuid_types.sql")
	if err != nil {
		t.Fatalf("expected migrations/004_fix_uuid_types.sql to exist: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "ALTER TABLE webhook_deliveries") {
		t.Errorf("migration missing webhook_deliveries upgrade")
	}
	if !strings.Contains(content, "ALTER TABLE pipeline_outbox") {
		t.Errorf("migration missing pipeline_outbox upgrade")
	}
	if !strings.Contains(content, "USING run_id::uuid") {
		t.Errorf("migration missing USING run_id::uuid casting")
	}
	if !strings.Contains(content, "table_schema = CURRENT_SCHEMA") {
		t.Errorf("migration missing CURRENT_SCHEMA check")
	}
	if !strings.Contains(content, "fk_webhook_deliveries_run_id") {
		t.Errorf("migration missing fk_webhook_deliveries_run_id constraint")
	}
}

func TestPostgres_MigrationUpgradeLive(t *testing.T) {
	s := testDB(t)
	migrationSQL, err := os.ReadFile("../../migrations/004_fix_uuid_types.sql")
	if err != nil {
		t.Fatalf("reading migration: %v", err)
	}
	if _, err := s.db.Exec(string(migrationSQL)); err != nil {
		t.Fatalf("executing migration 004 failed: %v", err)
	}
}
