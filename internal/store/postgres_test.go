//go:build postgres

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
)

func setConnSearchPath(connStr, schema string) string {
	searchPathVal := fmt.Sprintf("%s,public", schema)
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			q := u.Query()
			q.Set("search_path", searchPathVal)
			u.RawQuery = q.Encode()
			return u.String()
		}
	}
	return connStr + fmt.Sprintf(" search_path='%s'", searchPathVal)
}

func testDB(t *testing.T) *PostgresStore {
	t.Helper()
	connStr := os.Getenv("DREV_DB_URL")
	if connStr == "" {
		t.Skip("DREV_DB_URL not set — skipping postgres integration tests")
	}

	// 1. Generate unique schema name for isolation
	schema := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), rand.Intn(10000))

	// 2. Open admin connection to create isolated schema before store initialization
	adminDB, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Open admin postgres: %v", err)
	}
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE SCHEMA %q", schema)); err != nil {
		adminDB.Close()
		t.Fatalf("create test schema %q: %v", schema, err)
	}

	// 3. Connect store with search_path scoped to isolated schema first
	scopedConnStr := setConnSearchPath(connStr, schema)
	s, err := OpenPostgres(scopedConnStr)
	if err != nil {
		_, _ = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema))
		adminDB.Close()
		t.Fatalf("OpenPostgres: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
		_, _ = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema))
		_ = adminDB.Close()
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
	runPendingOutbox := &drevtypes.Run{
		ID:         "run-pg-ghost-pending-outbox",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delPendingOutbox := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-ghost-pending-outbox",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runPendingOutbox.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delPendingOutbox, runPendingOutbox, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(pending-outbox): %v", err)
	}

	runPendingNoOutbox := &drevtypes.Run{
		ID:         "run-pg-ghost-pending-no-outbox",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delPendingNoOutbox := &drevtypes.WebhookDelivery{
		DeliveryID: "del-pg-ghost-pending-no-outbox",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runPendingNoOutbox.ID,
	}
	if err := s.CreateRun(ctx, runPendingNoOutbox); err != nil {
		t.Fatalf("CreateRun(pending-no-outbox): %v", err)
	}
	if err := s.RecordDelivery(ctx, delPendingNoOutbox); err != nil {
		t.Fatalf("RecordDelivery(pending-no-outbox): %v", err)
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

	// 2. Call ResetGhostRuns: only runPendingNoOutbox and runRunning are ghost runs
	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns failed: %v", err)
	}
	if len(interrupted) != 2 {
		t.Fatalf("expected 2 interrupted runs, got %d: %v", len(interrupted), interrupted)
	}

	// 3. Verify interrupted runs are now failed
	rNoOutbox, _ := s.GetRun(ctx, runPendingNoOutbox.ID)
	if rNoOutbox.Status != drevtypes.StatusFailed || rNoOutbox.FinishedAt.IsZero() {
		t.Errorf("expected runPendingNoOutbox to be failed with finished_at, got: %+v", rNoOutbox)
	}
	r2, _ := s.GetRun(ctx, runRunning.ID)
	if r2.Status != drevtypes.StatusFailed || r2.FinishedAt.IsZero() {
		t.Errorf("expected runRunning to be failed with finished_at, got: %+v", r2)
	}

	// Pending run with outbox entry must SURVIVE as pending
	rOutbox, _ := s.GetRun(ctx, runPendingOutbox.ID)
	if rOutbox.Status != drevtypes.StatusPending {
		t.Errorf("expected runPendingOutbox to remain pending, got: %s", rOutbox.Status)
	}

	// 4. Verify webhook delivery records for interrupted runs are RELEASED
	dNoOutbox, err := s.GetDelivery(ctx, delPendingNoOutbox.DeliveryID)
	if err != nil || dNoOutbox != nil {
		t.Errorf("expected del-pg-ghost-pending-no-outbox to be released, got: %+v", dNoOutbox)
	}
	dRunning, err := s.GetDelivery(ctx, delRunning.DeliveryID)
	if err != nil || dRunning != nil {
		t.Errorf("expected del-pg-ghost-running to be released, got: %+v", dRunning)
	}

	// Delivery for surviving pending run with outbox is NOT released
	dOutbox, err := s.GetDelivery(ctx, delPendingOutbox.DeliveryID)
	if err != nil || dOutbox == nil {
		t.Errorf("expected del-pg-ghost-pending-outbox to remain intact, got: %+v (err: %v)", dOutbox, err)
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
		DeliveryID: delRunning.DeliveryID, // Reusing interrupted delivery ID
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runRetried.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delRetried, runRetried, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery after recovery failed: %v", err)
	}
	dFinal, _ := s.GetDelivery(ctx, delRunning.DeliveryID)
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

func TestPostgres_Migration005_FileExistsAndValid(t *testing.T) {
	data, err := os.ReadFile("../../migrations/005_nullable_run_org_id.sql")
	if err != nil {
		t.Fatalf("expected migrations/005_nullable_run_org_id.sql to exist: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "ALTER TABLE runs") {
		t.Errorf("migration missing ALTER TABLE runs")
	}
	if !strings.Contains(content, "ALTER COLUMN org_id DROP NOT NULL") {
		t.Errorf("migration missing ALTER COLUMN org_id DROP NOT NULL")
	}
	if !strings.Contains(content, "table_schema = CURRENT_SCHEMA") {
		t.Errorf("migration missing CURRENT_SCHEMA check")
	}
	if !strings.Contains(content, "column_name = 'org_id'") {
		t.Errorf("migration missing column_name = 'org_id' check")
	}
}

func TestPostgres_Migration005_UpgradeLive(t *testing.T) {
	connStr := os.Getenv("DREV_DB_URL")
	if connStr == "" {
		t.Skip("DREV_DB_URL not set — skipping postgres integration tests")
	}

	migrationSQL, err := os.ReadFile("../../migrations/005_nullable_run_org_id.sql")
	if err != nil {
		t.Fatalf("reading migration 005: %v", err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("opening postgres: %v", err)
	}
	defer db.Close()

	// Dedicated test schema for migration verification
	testSchema := fmt.Sprintf("test_mig005_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
	if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %q", testSchema)); err != nil {
		t.Fatalf("creating test schema %q: %v", testSchema, err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", testSchema))
	})

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquiring dedicated connection: %v", err)
	}
	defer conn.Close()

	// Set search_path strictly to testSchema (excluding public) so CURRENT_SCHEMA is testSchema
	// and no fallback can ever resolve or affect public tables.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET search_path TO %q", testSchema)); err != nil {
		t.Fatalf("setting dedicated search_path: %v", err)
	}

	// Create test runs table in CURRENT_SCHEMA with NOT NULL org_id
	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE runs (
			id     UUID PRIMARY KEY,
			org_id UUID NOT NULL
		)`); err != nil {
		t.Fatalf("creating test runs table with NOT NULL org_id: %v", err)
	}

	checkNullable := func() string {
		var isNullable string
		err := conn.QueryRowContext(ctx, `
			SELECT is_nullable
			FROM information_schema.columns
			WHERE table_schema = CURRENT_SCHEMA
			  AND table_name = 'runs'
			  AND column_name = 'org_id'`).Scan(&isNullable)
		if err != nil {
			t.Fatalf("querying is_nullable from information_schema: %v", err)
		}
		return isNullable
	}

	// 1. Verify that initially runs.org_id is NOT NULL (is_nullable = 'NO')
	if isNullable := checkNullable(); isNullable != "NO" {
		t.Fatalf("expected initial runs.org_id to be NOT NULL (is_nullable='NO'), got: %s", isNullable)
	}

	// 2. Execute migration 005 to upgrade schema
	if _, err := conn.ExecContext(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("executing migration 005 failed: %v", err)
	}

	// 3. Verify that runs.org_id is now nullable (is_nullable = 'YES')
	if isNullable := checkNullable(); isNullable != "YES" {
		t.Errorf("expected runs.org_id to be nullable after migration (is_nullable='YES'), got: %s", isNullable)
	}

	// 4. Re-execute migration 005 to prove idempotency
	if _, err := conn.ExecContext(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("re-executing migration 005 (idempotency check) failed: %v", err)
	}

	// 5. Verify that runs.org_id remains nullable (is_nullable = 'YES')
	if isNullable := checkNullable(); isNullable != "YES" {
		t.Errorf("expected runs.org_id to remain nullable after idempotent rerun (is_nullable='YES'), got: %s", isNullable)
	}
}

func TestPostgresStore_CreateRunJobs_Success(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := "run-pg-batch-001"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-pg-1",
		Status:     drevtypes.StatusRunning,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	jobs := []*drevtypes.RunJob{
		{
			ID:         "job-pg-b-1",
			RunID:      runID,
			JobName:    "build",
			Status:     drevtypes.StatusRunning,
			StartedAt:  now,
			FinishedAt: now.Add(time.Minute),
		},
		{
			ID:      "job-pg-b-2",
			RunID:   runID,
			JobName: "test",
			Status:  "", // Defaults to pending
		},
	}

	if err := s.CreateRunJobs(ctx, runID, jobs); err != nil {
		t.Fatalf("CreateRunJobs: %v", err)
	}

	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(gotJobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(gotJobs))
	}
}

func TestPostgresStore_CreateRunJobs_Validation(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := "run-pg-val-001"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p1", Status: drevtypes.StatusPending}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Blank runID
	if err := s.CreateRunJobs(ctx, "", []*drevtypes.RunJob{{ID: "j1", RunID: "", JobName: "b"}}); err == nil {
		t.Error("expected error on blank runID")
	}

	// Empty jobs slice
	if err := s.CreateRunJobs(ctx, runID, []*drevtypes.RunJob{}); err == nil {
		t.Error("expected error on empty jobs slice")
	}

	// Nil jobs slice
	if err := s.CreateRunJobs(ctx, runID, nil); err == nil {
		t.Error("expected error on nil jobs slice")
	}

	// Nil job entry
	if err := s.CreateRunJobs(ctx, runID, []*drevtypes.RunJob{nil}); err == nil {
		t.Error("expected error on nil job entry")
	}

	// Mismatched RunID
	if err := s.CreateRunJobs(ctx, runID, []*drevtypes.RunJob{{ID: "j1", RunID: "other-run", JobName: "b"}}); err == nil {
		t.Error("expected error on mismatched RunID")
	}

	// Duplicate job ID
	if err := s.CreateRunJobs(ctx, runID, []*drevtypes.RunJob{
		{ID: "dup-1", RunID: runID, JobName: "b1"},
		{ID: "dup-1", RunID: runID, JobName: "b2"},
	}); err == nil {
		t.Error("expected error on duplicate job ID")
	}

	// Duplicate job name
	if err := s.CreateRunJobs(ctx, runID, []*drevtypes.RunJob{
		{ID: "j1", RunID: runID, JobName: "same"},
		{ID: "j2", RunID: runID, JobName: "same"},
	}); err == nil {
		t.Error("expected error on duplicate job name")
	}

	// Nonexistent run
	if err := s.CreateRunJobs(ctx, "run-nonexistent", []*drevtypes.RunJob{{ID: "j1", RunID: "run-nonexistent", JobName: "b"}}); err == nil {
		t.Error("expected error on nonexistent run")
	}
}

func TestPostgresStore_CreateRunJobs_Atomicity_RollbackOnConstraintFailure(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := "run-pg-rb-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p-rb", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// 1. Seed existing job
	seededJobID := "job-pg-seeded-1"
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:      seededJobID,
		RunID:   runID,
		JobName: "setup",
		Status:  drevtypes.StatusSuccess,
	}); err != nil {
		t.Fatalf("CreateRunJob(seeded): %v", err)
	}

	// 2. Submit batch with new job 1 and duplicate-ID job 2
	newJobID := "job-pg-new-1"
	failingBatch := []*drevtypes.RunJob{
		{
			ID:      newJobID,
			RunID:   runID,
			JobName: "build-step",
			Status:  drevtypes.StatusPending,
		},
		{
			ID:      seededJobID, // duplicate primary key in database
			RunID:   runID,
			JobName: "test-step",
			Status:  drevtypes.StatusPending,
		},
	}

	err := s.CreateRunJobs(ctx, runID, failingBatch)
	if err == nil {
		t.Fatal("expected database constraint error on duplicate job ID, got nil")
	}

	// 3. PROVE ROLLBACK: newJobID must not exist in the database
	jobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != seededJobID {
		t.Fatalf("expected only seeded job, found %d jobs", len(jobs))
	}

	// 4. Retry with valid batch succeeds
	validBatch := []*drevtypes.RunJob{
		{
			ID:      newJobID,
			RunID:   runID,
			JobName: "build-step",
			Status:  drevtypes.StatusPending,
		},
		{
			ID:      "job-pg-valid-2",
			RunID:   runID,
			JobName: "test-step",
			Status:  drevtypes.StatusPending,
		},
	}
	if err := s.CreateRunJobs(ctx, runID, validBatch); err != nil {
		t.Fatalf("retry CreateRunJobs failed: %v", err)
	}

	finalJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil || len(finalJobs) != 3 {
		t.Fatalf("expected 3 jobs after retry, got %d (err: %v)", len(finalJobs), err)
	}
}

func TestPostgresStore_CreateRunJob_Compatible(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := "run-pg-compat-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p-c", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	job := &drevtypes.RunJob{
		ID:      "pg-single-1",
		RunID:   runID,
		JobName: "compile",
		Status:  drevtypes.StatusPending,
	}

	if err := s.CreateRunJob(ctx, job); err != nil {
		t.Fatalf("CreateRunJob: %v", err)
	}

	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil || len(gotJobs) != 1 {
		t.Fatalf("expected 1 job, got %d (err: %v)", len(gotJobs), err)
	}
	if gotJobs[0].ID != "pg-single-1" {
		t.Errorf("expected pg-single-1, got %s", gotJobs[0].ID)
	}
}

func TestPostgresStore_ResetGhostRuns_OutboxAwareness(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Pending run WITH outbox entry
	runPendingWithOutbox := &drevtypes.Run{
		ID:         "run-pg-pending-with-outbox",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del1 := &drevtypes.WebhookDelivery{DeliveryID: "del-pg-1", RunID: runPendingWithOutbox.ID}
	if err := s.CreateRunWithDelivery(ctx, del1, runPendingWithOutbox, nil, `{"name":"p1"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(pending-with-outbox): %v", err)
	}

	// 2. Pending run WITHOUT outbox entry (e.g. legacy/API trigger)
	runPendingNoOutbox := &drevtypes.Run{
		ID:         "run-pg-pending-no-outbox",
		PipelineID: "p2",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(ctx, runPendingNoOutbox); err != nil {
		t.Fatalf("CreateRun(pending-no-outbox): %v", err)
	}

	// 3. Running run WITH outbox entry (e.g. claim interrupted before outbox delete)
	runRunningWithOutbox := &drevtypes.Run{
		ID:         "run-pg-running-with-outbox",
		PipelineID: "p3",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now(),
	}
	del3 := &drevtypes.WebhookDelivery{DeliveryID: "del-pg-3", RunID: runRunningWithOutbox.ID}
	if err := s.CreateRunWithDelivery(ctx, del3, runRunningWithOutbox, nil, `{"name":"p3"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(running-with-outbox): %v", err)
	}

	// 4. Running run WITHOUT outbox entry (standard claimed executing run)
	runRunningNoOutbox := &drevtypes.Run{
		ID:         "run-pg-running-no-outbox",
		PipelineID: "p4",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now(),
	}
	if err := s.CreateRun(ctx, runRunningNoOutbox); err != nil {
		t.Fatalf("CreateRun(running-no-outbox): %v", err)
	}

	// 5. Terminal runs: success, failed, cancelled
	runSuccess := &drevtypes.Run{
		ID:         "run-pg-terminal-success",
		PipelineID: "p5",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  time.Now().Add(-10 * time.Minute),
		FinishedAt: time.Now().Add(-5 * time.Minute),
	}
	runFailed := &drevtypes.Run{
		ID:         "run-pg-terminal-failed",
		PipelineID: "p6",
		Status:     drevtypes.StatusFailed,
		StartedAt:  time.Now().Add(-10 * time.Minute),
		FinishedAt: time.Now().Add(-5 * time.Minute),
	}
	runCancelled := &drevtypes.Run{
		ID:         "run-pg-terminal-cancelled",
		PipelineID: "p7",
		Status:     drevtypes.StatusCancelled,
		StartedAt:  time.Now().Add(-10 * time.Minute),
		FinishedAt: time.Now().Add(-5 * time.Minute),
	}
	for _, term := range []*drevtypes.Run{runSuccess, runFailed, runCancelled} {
		if err := s.CreateRun(ctx, term); err != nil {
			t.Fatalf("CreateRun(%s): %v", term.ID, err)
		}
	}

	// Execute ResetGhostRuns
	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns failed: %v", err)
	}

	// Verify exact returned IDs:
	// - runPendingNoOutbox (pending, no outbox) -> must be reset
	// - runRunningWithOutbox (running, with outbox) -> must be reset
	// - runRunningNoOutbox (running, no outbox) -> must be reset
	// - runPendingWithOutbox (pending, with outbox) -> MUST SURVIVE (not reset)
	// - terminal runs -> MUST SURVIVE (not reset)
	if len(interrupted) != 3 {
		t.Fatalf("expected 3 interrupted runs, got %d: %v", len(interrupted), interrupted)
	}

	interruptedSet := make(map[string]bool)
	for _, id := range interrupted {
		interruptedSet[id] = true
	}

	if interruptedSet["run-pg-pending-with-outbox"] {
		t.Errorf("INVARIANT VIOLATION: pending run with outbox was marked as interrupted")
	}
	if !interruptedSet["run-pg-pending-no-outbox"] {
		t.Errorf("expected pending run without outbox to be interrupted")
	}
	if !interruptedSet["run-pg-running-with-outbox"] {
		t.Errorf("expected running run with outbox to be interrupted")
	}
	if !interruptedSet["run-pg-running-no-outbox"] {
		t.Errorf("expected running run without outbox to be interrupted")
	}

	// Verify DB statuses
	r1, _ := s.GetRun(ctx, "run-pg-pending-with-outbox")
	if r1.Status != drevtypes.StatusPending {
		t.Errorf("expected run-pg-pending-with-outbox to remain pending, got: %s", r1.Status)
	}

	r2, _ := s.GetRun(ctx, "run-pg-pending-no-outbox")
	if r2.Status != drevtypes.StatusFailed || r2.FinishedAt.IsZero() {
		t.Errorf("expected run-pg-pending-no-outbox to be failed with finished_at, got: %+v", r2)
	}

	r3, _ := s.GetRun(ctx, "run-pg-running-with-outbox")
	if r3.Status != drevtypes.StatusFailed || r3.FinishedAt.IsZero() {
		t.Errorf("expected run-pg-running-with-outbox to be failed with finished_at, got: %+v", r3)
	}

	r4, _ := s.GetRun(ctx, "run-pg-running-no-outbox")
	if r4.Status != drevtypes.StatusFailed || r4.FinishedAt.IsZero() {
		t.Errorf("expected run-pg-running-no-outbox to be failed with finished_at, got: %+v", r4)
	}

	// Terminal runs must remain unchanged
	rSucc, _ := s.GetRun(ctx, "run-pg-terminal-success")
	if rSucc.Status != drevtypes.StatusSuccess {
		t.Errorf("expected run-pg-terminal-success to remain success, got: %s", rSucc.Status)
	}
	rFail, _ := s.GetRun(ctx, "run-pg-terminal-failed")
	if rFail.Status != drevtypes.StatusFailed {
		t.Errorf("expected run-pg-terminal-failed to remain failed, got: %s", rFail.Status)
	}
	rCanc, _ := s.GetRun(ctx, "run-pg-terminal-cancelled")
	if rCanc.Status != drevtypes.StatusCancelled {
		t.Errorf("expected run-pg-terminal-cancelled to remain cancelled, got: %s", rCanc.Status)
	}

	// Outbox entries must be completely preserved
	pendingOutbox, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox failed: %v", err)
	}
	outboxRunIDs := make(map[string]bool)
	for _, item := range pendingOutbox {
		outboxRunIDs[item.RunID] = true
	}
	if !outboxRunIDs["run-pg-pending-with-outbox"] {
		t.Errorf("outbox entry for run-pg-pending-with-outbox was unexpectedly removed")
	}
}

func TestPostgresStore_CreateRunWithDelivery_ForeignKeyOrder_RealUUID(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// ── Case 1: Webhook ingestion with absent OrgID and human-readable PipelineID ──
	// Verifies parent-before-child foreign key ordering, and verifies that absent
	// OrgID is persisted as SQL NULL, pipeline_name stores the human-readable name,
	// runs.pipeline_id remains SQL NULL, and GetRun returns that exact pipeline name.
	runID := uuid.New().String()
	deliveryID := "del-pg-uuid-" + uuid.New().String()

	del := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "octocat/hello-world",
		PRNumber:   0,
		RunID:      runID,
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}

	run := &drevtypes.Run{
		ID:          runID,
		OrgID:       "",
		PipelineID:  "deploy-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
		CommitSHA:   "1234567890abcdef1234567890abcdef12345678",
		Branch:      "main",
	}

	job1ID := uuid.New().String()
	job2ID := uuid.New().String()
	jobs := []*drevtypes.RunJob{
		{ID: job1ID, RunID: runID, JobName: "build", Status: drevtypes.StatusPending},
		{ID: job2ID, RunID: runID, JobName: "test", Status: drevtypes.StatusPending},
	}

	pipelineSpec := `{"name":"deploy-pipeline","jobs":[{"name":"build"},{"name":"test"}]}`

	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, pipelineSpec); err != nil {
		t.Fatalf("CreateRunWithDelivery with absent OrgID failed: %v", err)
	}

	// 1. Verify delivery exists and references runID
	gotDel, err := s.GetDelivery(ctx, deliveryID)
	if err != nil || gotDel == nil {
		t.Fatalf("expected delivery in store, got %+v (err: %v)", gotDel, err)
	}
	if gotDel.RunID != runID {
		t.Errorf("expected delivery.RunID = %s, got %s", runID, gotDel.RunID)
	}

	// 2. Verify GetRun returns empty OrgID and the human-readable PipelineID
	gotRun, err := s.GetRun(ctx, runID)
	if err != nil || gotRun == nil {
		t.Fatalf("expected run in store, got %+v (err: %v)", gotRun, err)
	}
	if gotRun.OrgID != "" {
		t.Errorf("expected run.OrgID to be empty string, got %q", gotRun.OrgID)
	}
	if gotRun.PipelineID != "deploy-pipeline" {
		t.Errorf("expected run.PipelineID to be %q, got %q", "deploy-pipeline", gotRun.PipelineID)
	}
	if gotRun.Status != drevtypes.StatusPending {
		t.Errorf("expected run.Status = pending, got %s", gotRun.Status)
	}

	// 3. Verify in PostgreSQL directly that org_id and pipeline_id are SQL NULL, and pipeline_name matches
	var dbOrgID, dbPipelineID sql.NullString
	var dbPipelineName string
	if err := s.db.QueryRowContext(ctx, `
		SELECT org_id, pipeline_id, pipeline_name
		FROM runs
		WHERE id = $1`, runID).Scan(&dbOrgID, &dbPipelineID, &dbPipelineName); err != nil {
		t.Fatalf("querying raw runs row: %v", err)
	}
	if dbOrgID.Valid {
		t.Errorf("expected runs.org_id to be SQL NULL, got: %s", dbOrgID.String)
	}
	if dbPipelineID.Valid {
		t.Errorf("expected runs.pipeline_id to be SQL NULL, got: %s", dbPipelineID.String)
	}
	if dbPipelineName != "deploy-pipeline" {
		t.Errorf("expected runs.pipeline_name = %q, got: %q", "deploy-pipeline", dbPipelineName)
	}

	// 4. Verify jobs exist
	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil || len(gotJobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d (err: %v)", len(gotJobs), err)
	}

	// 5. Verify outbox exists
	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(outboxItems) != 1 || outboxItems[0].RunID != runID {
		t.Fatalf("expected outbox item for %s, got %+v (err: %v)", runID, outboxItems, err)
	}

	// ── Case 2: Webhook ingestion with absent OrgID and empty PipelineID ────────
	runEmptyID := uuid.New().String()
	delEmptyID := "del-pg-empty-" + uuid.New().String()
	delEmpty := &drevtypes.WebhookDelivery{
		DeliveryID: delEmptyID,
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      runEmptyID,
	}
	runEmpty := &drevtypes.Run{
		ID:          runEmptyID,
		OrgID:       "",
		PipelineID:  "", // empty pipeline name
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}

	if err := s.CreateRunWithDelivery(ctx, delEmpty, runEmpty, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery with empty PipelineID failed: %v", err)
	}

	gotRunEmpty, err := s.GetRun(ctx, runEmptyID)
	if err != nil || gotRunEmpty == nil {
		t.Fatalf("expected runEmpty in store: %v", err)
	}
	if gotRunEmpty.PipelineID != "" {
		t.Errorf("expected empty PipelineID, got %q", gotRunEmpty.PipelineID)
	}

	var dbEmptyOrgID, dbEmptyPipelineID sql.NullString
	var dbEmptyPipelineName string
	if err := s.db.QueryRowContext(ctx, `
		SELECT org_id, pipeline_id, pipeline_name
		FROM runs
		WHERE id = $1`, runEmptyID).Scan(&dbEmptyOrgID, &dbEmptyPipelineID, &dbEmptyPipelineName); err != nil {
		t.Fatalf("querying raw runEmpty row: %v", err)
	}
	if dbEmptyOrgID.Valid {
		t.Errorf("expected runs.org_id to be SQL NULL, got: %s", dbEmptyOrgID.String)
	}
	if dbEmptyPipelineID.Valid {
		t.Errorf("expected runs.pipeline_id to be SQL NULL, got: %s", dbEmptyPipelineID.String)
	}
	if dbEmptyPipelineName != "" {
		t.Errorf("expected runs.pipeline_name = %q, got: %q", "", dbEmptyPipelineName)
	}

	// ── Case 3: Satisfy referenced organization foreign key with real UUID ──────
	orgID := uuid.New().String()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO organizations (id, name, slug)
		VALUES ($1, $2, $3)`,
		orgID, "Test Org", "test-org-"+uuid.New().String(),
	); err != nil {
		t.Fatalf("inserting test organization: %v", err)
	}

	runOrgID := uuid.New().String()
	delOrgID := "del-org-" + uuid.New().String()
	delOrg := &drevtypes.WebhookDelivery{
		DeliveryID: delOrgID,
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      runOrgID,
	}
	runOrg := &drevtypes.Run{
		ID:          runOrgID,
		OrgID:       orgID,
		PipelineID:  "org-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}

	if err := s.CreateRunWithDelivery(ctx, delOrg, runOrg, nil, `{"name":"org-pipeline"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery with valid organization foreign key failed: %v", err)
	}

	gotRunOrg, err := s.GetRun(ctx, runOrgID)
	if err != nil || gotRunOrg == nil {
		t.Fatalf("expected runOrg in store: %v", err)
	}
	if gotRunOrg.OrgID != orgID {
		t.Errorf("expected runOrg.OrgID = %s, got %s", orgID, gotRunOrg.OrgID)
	}
	if gotRunOrg.PipelineID != "org-pipeline" {
		t.Errorf("expected runOrg.PipelineID = %q, got %q", "org-pipeline", gotRunOrg.PipelineID)
	}

	var fkOrgID, fkPipelineID sql.NullString
	var fkPipelineName string
	if err := s.db.QueryRowContext(ctx, `
		SELECT org_id, pipeline_id, pipeline_name
		FROM runs
		WHERE id = $1`, runOrgID).Scan(&fkOrgID, &fkPipelineID, &fkPipelineName); err != nil {
		t.Fatalf("querying raw FK runs row: %v", err)
	}
	if !fkOrgID.Valid || fkOrgID.String != orgID {
		t.Errorf("expected runs.org_id = %s, got: %+v", orgID, fkOrgID)
	}
	if fkPipelineID.Valid {
		t.Errorf("expected runs.pipeline_id to be SQL NULL, got: %s", fkPipelineID.String)
	}
	if fkPipelineName != "org-pipeline" {
		t.Errorf("expected runs.pipeline_name = %q, got: %q", "org-pipeline", fkPipelineName)
	}
}

func TestPostgresStore_CreateRunWithDelivery_DuplicateDelivery_RollsBackRun(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run1ID := uuid.New().String()
	deliveryID := "del-pg-dup-" + uuid.New().String()

	del1 := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run1ID,
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}
	run1 := &drevtypes.Run{
		ID:          run1ID,
		OrgID:       "",
		PipelineID:  "ci",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}

	if err := s.CreateRunWithDelivery(ctx, del1, run1, nil, `{"name":"ci"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(1) failed: %v", err)
	}

	// Attempt duplicate delivery with SAME deliveryID but a NEW run UUID
	run2ID := uuid.New().String()
	delDup := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run2ID,
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}
	runDup := &drevtypes.Run{
		ID:          run2ID,
		OrgID:       "",
		PipelineID:  "ci-dup",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}

	err := s.CreateRunWithDelivery(ctx, delDup, runDup, nil, `{"name":"ci-dup"}`)
	if err == nil {
		t.Fatalf("expected ErrDuplicateDelivery on duplicate delivery ID, got nil")
	}
	if !errors.Is(err, ErrDuplicateDelivery) {
		t.Fatalf("expected ErrDuplicateDelivery, got: %v", err)
	}

	// PROVE ROLLBACK: runDup must NOT exist in the database!
	gotRunDup, err := s.GetRun(ctx, run2ID)
	if err == nil && gotRunDup != nil {
		t.Fatalf("expected runDup (%s) to be rolled back, found in database: %+v", run2ID, gotRunDup)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE id = $1`, run2ID).Scan(&count); err != nil {
		t.Fatalf("counting rolled back run rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows for rolled back run %s, found %d", run2ID, count)
	}

	// Verify original delivery and run remain intact
	delCheck, err := s.GetDelivery(ctx, deliveryID)
	if err != nil || delCheck == nil || delCheck.RunID != run1ID {
		t.Fatalf("expected original delivery to point to run1 (%s), got: %+v (err: %v)", run1ID, delCheck, err)
	}
	run1Check, err := s.GetRun(ctx, run1ID)
	if err != nil || run1Check == nil || run1Check.Status != drevtypes.StatusPending {
		t.Fatalf("expected original run1 to remain pending, got: %+v (err: %v)", run1Check, err)
	}
}

func TestPostgresStore_CreateRunWithLimit_ConcurrentRequests_CannotExceedLimit(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// Seed organization with real UUID
	orgID := uuid.New().String()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO organizations (id, name, slug)
		VALUES ($1, $2, $3)`,
		orgID, "Test Concurrent Org", "test-conc-"+uuid.New().String(),
	); err != nil {
		t.Fatalf("inserting test organization: %v", err)
	}

	workerLimit := 2
	concurrency := 10

	var wg sync.WaitGroup
	var successCount int64
	var limitExceededCount int64
	var otherErrCount int64

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			runID := uuid.New().String()
			run := &drevtypes.Run{
				ID:          runID,
				OrgID:       orgID,
				PipelineID:  "ci",
				Status:      drevtypes.StatusPending,
				TriggeredBy: "api",
			}
			jobs := []*drevtypes.RunJob{
				{ID: uuid.New().String(), RunID: runID, JobName: "build", Status: drevtypes.StatusPending},
			}
			err := s.CreateRunWithLimit(ctx, run, jobs, workerLimit)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else if errors.Is(err, ErrWorkerLimitExceeded) {
				atomic.AddInt64(&limitExceededCount, 1)
			} else {
				atomic.AddInt64(&otherErrCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected error during concurrent CreateRunWithLimit: %d", otherErrCount)
	}
	if successCount != int64(workerLimit) {
		t.Errorf("expected %d successes, got %d", workerLimit, successCount)
	}
	if limitExceededCount != int64(concurrency-workerLimit) {
		t.Errorf("expected %d limit exceeded, got %d", concurrency-workerLimit, limitExceededCount)
	}

	// Verify database state: exactly 2 runs exist for orgID
	var activeCount int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runs WHERE org_id = $1 AND status IN ('running', 'pending')`, orgID).Scan(&activeCount); err != nil {
		t.Fatalf("counting active runs: %v", err)
	}
	if activeCount != workerLimit {
		t.Errorf("expected %d active runs in database, got %d", workerLimit, activeCount)
	}
}

func TestPostgresStore_CreateRunWithLimit_IndependentTenantAllowances(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// Org A
	orgA := uuid.New().String()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)`, orgA, "Org A", "org-a-"+uuid.New().String()); err != nil {
		t.Fatalf("inserting orgA: %v", err)
	}
	// Org B
	orgB := uuid.New().String()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)`, orgB, "Org B", "org-b-"+uuid.New().String()); err != nil {
		t.Fatalf("inserting orgB: %v", err)
	}

	// Org A uses 2 runs (limit 2)
	for i := 0; i < 2; i++ {
		runID := uuid.New().String()
		err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
			ID: runID, OrgID: orgA, PipelineID: "pA", Status: drevtypes.StatusPending,
		}, nil, 2)
		if err != nil {
			t.Fatalf("CreateRunWithLimit for orgA (%d) failed: %v", i, err)
		}
	}

	// Org B uses 3 runs (limit 3) - completely unaffected by Org A
	for i := 0; i < 3; i++ {
		runID := uuid.New().String()
		err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
			ID: runID, OrgID: orgB, PipelineID: "pB", Status: drevtypes.StatusPending,
		}, nil, 3)
		if err != nil {
			t.Fatalf("CreateRunWithLimit for orgB (%d) failed: %v", i, err)
		}
	}

	// Org A next attempt fails with ErrWorkerLimitExceeded
	errA := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: uuid.New().String(), OrgID: orgA, PipelineID: "pA", Status: drevtypes.StatusPending,
	}, nil, 2)
	if !errors.Is(errA, ErrWorkerLimitExceeded) {
		t.Fatalf("expected ErrWorkerLimitExceeded for orgA, got: %v", errA)
	}

	// Org B next attempt fails with ErrWorkerLimitExceeded
	errB := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: uuid.New().String(), OrgID: orgB, PipelineID: "pB", Status: drevtypes.StatusPending,
	}, nil, 3)
	if !errors.Is(errB, ErrWorkerLimitExceeded) {
		t.Fatalf("expected ErrWorkerLimitExceeded for orgB, got: %v", errB)
	}
}

func TestPostgresStore_CreateRunWithLimit_MissingOrgReturnsError(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	nonExistentOrg := uuid.New().String()
	runID := uuid.New().String()
	err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: runID, OrgID: nonExistentOrg, PipelineID: "p1", Status: drevtypes.StatusPending,
	}, nil, 2)
	if err == nil {
		t.Fatalf("expected ErrOrgNotFound for missing org, got nil")
	}
	if !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("expected ErrOrgNotFound, got: %v", err)
	}
}

func TestPostgresStore_CreateRunWithLimit_TerminalRunsFreeCapacity(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	orgID := uuid.New().String()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)`, orgID, "Org Term", "org-term-"+uuid.New().String()); err != nil {
		t.Fatalf("inserting org: %v", err)
	}

	run1ID := uuid.New().String()
	if err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: run1ID, OrgID: orgID, PipelineID: "pTerm", Status: drevtypes.StatusPending,
	}, nil, 1); err != nil {
		t.Fatalf("CreateRunWithLimit(1): %v", err)
	}

	// Next run rejected
	run2ID := uuid.New().String()
	err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: run2ID, OrgID: orgID, PipelineID: "pTerm", Status: drevtypes.StatusPending,
	}, nil, 1)
	if !errors.Is(err, ErrWorkerLimitExceeded) {
		t.Fatalf("expected ErrWorkerLimitExceeded, got: %v", err)
	}

	// Mark run 1 as Success
	if err := s.UpdateRunStatus(ctx, run1ID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("UpdateRunStatus: %v", err)
	}

	// Capacity recovered; run 2 now succeeds
	if err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: run2ID, OrgID: orgID, PipelineID: "pTerm", Status: drevtypes.StatusPending,
	}, nil, 1); err != nil {
		t.Fatalf("expected success after terminal status, got: %v", err)
	}
}

func TestPostgresStore_CreateRunWithLimit_TenantSafety_EmptyOrgID(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// workerLimit > 0 with empty OrgID fails closed
	err := s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: uuid.New().String(), OrgID: "", PipelineID: "pEmpty", Status: drevtypes.StatusPending,
	}, nil, 2)
	if err == nil {
		t.Fatalf("expected error for empty OrgID with workerLimit > 0, got nil")
	}

	// workerLimit == 0 with empty OrgID succeeds (single tenant)
	runSingleID := uuid.New().String()
	err = s.CreateRunWithLimit(ctx, &drevtypes.Run{
		ID: runSingleID, OrgID: "", PipelineID: "pSingle", Status: drevtypes.StatusPending,
	}, nil, 0)
	if err != nil {
		t.Fatalf("expected success for workerLimit == 0: %v", err)
	}
}

func TestPostgresStore_TerminalRunStatus_CannotBeOverwritten(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Cancelled run cannot become successful
	runCancelled := &drevtypes.Run{
		ID:         uuid.New().String(),
		PipelineID: "p1",
		Status:     drevtypes.StatusCancelled,
	}
	if err := s.CreateRun(ctx, runCancelled); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	err := s.UpdateRunStatus(ctx, runCancelled.ID, drevtypes.StatusSuccess)
	if err == nil || !errors.Is(err, ErrRunAlreadyTerminal) {
		t.Fatalf("expected ErrRunAlreadyTerminal when transitioning cancelled -> success, got: %v", err)
	}
	got, _ := s.GetRun(ctx, runCancelled.ID)
	if got.Status != drevtypes.StatusCancelled {
		t.Errorf("status was overwritten! got %s, want %s", got.Status, drevtypes.StatusCancelled)
	}

	// 2. Failed run cannot become successful
	runFailed := &drevtypes.Run{
		ID:         uuid.New().String(),
		PipelineID: "p1",
		Status:     drevtypes.StatusFailed,
	}
	if err := s.CreateRun(ctx, runFailed); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	err = s.UpdateRunStatus(ctx, runFailed.ID, drevtypes.StatusSuccess)
	if err == nil || !errors.Is(err, ErrRunAlreadyTerminal) {
		t.Fatalf("expected ErrRunAlreadyTerminal when transitioning failed -> success, got: %v", err)
	}
	got, _ = s.GetRun(ctx, runFailed.ID)
	if got.Status != drevtypes.StatusFailed {
		t.Errorf("status was overwritten! got %s, want %s", got.Status, drevtypes.StatusFailed)
	}

	// 3. Successful run cannot become failed
	runSuccess := &drevtypes.Run{
		ID:         uuid.New().String(),
		PipelineID: "p1",
		Status:     drevtypes.StatusSuccess,
	}
	if err := s.CreateRun(ctx, runSuccess); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	err = s.UpdateRunStatus(ctx, runSuccess.ID, drevtypes.StatusFailed)
	if err == nil || !errors.Is(err, ErrRunAlreadyTerminal) {
		t.Fatalf("expected ErrRunAlreadyTerminal when transitioning success -> failed, got: %v", err)
	}
	got, _ = s.GetRun(ctx, runSuccess.ID)
	if got.Status != drevtypes.StatusSuccess {
		t.Errorf("status was overwritten! got %s, want %s", got.Status, drevtypes.StatusSuccess)
	}
}

func TestPostgresStore_TerminalRunStatus_IdempotentUpdatesPreserveTimestamps(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         uuid.New().String(),
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// First transition to cancelled
	if err := s.UpdateRunStatus(ctx, run.ID, drevtypes.StatusCancelled); err != nil {
		t.Fatalf("UpdateRunStatus to cancelled: %v", err)
	}
	initial, _ := s.GetRun(ctx, run.ID)
	if initial.FinishedAt.IsZero() {
		t.Fatal("expected finished_at to be set on cancelled")
	}

	time.Sleep(10 * time.Millisecond)

	// Idempotent transition to cancelled must succeed and not overwrite finished_at
	if err := s.UpdateRunStatus(ctx, run.ID, drevtypes.StatusCancelled); err != nil {
		t.Fatalf("idempotent UpdateRunStatus failed: %v", err)
	}
	after, _ := s.GetRun(ctx, run.ID)
	if !after.FinishedAt.Equal(initial.FinishedAt) {
		t.Errorf("finished_at timestamp changed on idempotent update: before %v, after %v", initial.FinishedAt, after.FinishedAt)
	}
}

func TestPostgresStore_LegitimateRunLifecycleTransitions(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// Path 1: pending -> running -> success
	r1ID := uuid.New().String()
	r1 := &drevtypes.Run{ID: r1ID, PipelineID: "p", Status: drevtypes.StatusPending}
	_ = s.CreateRun(ctx, r1)
	if err := s.UpdateRunStatus(ctx, r1.ID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("pending -> running: %v", err)
	}
	if err := s.UpdateRunStatus(ctx, r1.ID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("running -> success: %v", err)
	}
	g1, _ := s.GetRun(ctx, r1.ID)
	if g1.Status != drevtypes.StatusSuccess || g1.StartedAt.IsZero() || g1.FinishedAt.IsZero() {
		t.Errorf("invalid state or timestamps: %+v", g1)
	}

	// Path 2: pending -> running -> failed
	r2ID := uuid.New().String()
	r2 := &drevtypes.Run{ID: r2ID, PipelineID: "p", Status: drevtypes.StatusPending}
	_ = s.CreateRun(ctx, r2)
	if err := s.UpdateRunStatus(ctx, r2.ID, drevtypes.StatusRunning); err != nil {
		t.Fatalf("pending -> running: %v", err)
	}
	if err := s.UpdateRunStatus(ctx, r2.ID, drevtypes.StatusFailed); err != nil {
		t.Fatalf("running -> failed: %v", err)
	}
	g2, _ := s.GetRun(ctx, r2.ID)
	if g2.Status != drevtypes.StatusFailed || g2.StartedAt.IsZero() || g2.FinishedAt.IsZero() {
		t.Errorf("invalid state or timestamps: %+v", g2)
	}

	// Path 3: pending -> cancelled
	r3ID := uuid.New().String()
	r3 := &drevtypes.Run{ID: r3ID, PipelineID: "p", Status: drevtypes.StatusPending}
	_ = s.CreateRun(ctx, r3)
	if err := s.UpdateRunStatus(ctx, r3.ID, drevtypes.StatusCancelled); err != nil {
		t.Fatalf("pending -> cancelled: %v", err)
	}
	g3, _ := s.GetRun(ctx, r3.ID)
	if g3.Status != drevtypes.StatusCancelled || g3.FinishedAt.IsZero() {
		t.Errorf("invalid state or timestamps: %+v", g3)
	}
}

func TestPostgresStore_TerminalJobStatus_CannotBeOverwritten(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := uuid.New().String()
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p", Status: drevtypes.StatusRunning})

	// 1. Cancelled job cannot become successful
	jobCancelledID := uuid.New().String()
	jobCancelled := &drevtypes.RunJob{ID: jobCancelledID, RunID: runID, JobName: "build", Status: drevtypes.StatusCancelled}
	_ = s.CreateRunJob(ctx, jobCancelled)
	err := s.UpdateRunJobStatus(ctx, jobCancelled.ID, drevtypes.StatusSuccess)
	if err == nil || !errors.Is(err, ErrJobAlreadyTerminal) {
		t.Fatalf("expected ErrJobAlreadyTerminal when transitioning cancelled -> success, got: %v", err)
	}

	// 2. Skipped job cannot become successful
	jobSkippedID := uuid.New().String()
	jobSkipped := &drevtypes.RunJob{ID: jobSkippedID, RunID: runID, JobName: "deploy", Status: drevtypes.StatusSkipped}
	_ = s.CreateRunJob(ctx, jobSkipped)
	err = s.UpdateRunJobStatus(ctx, jobSkipped.ID, drevtypes.StatusSuccess)
	if err == nil || !errors.Is(err, ErrJobAlreadyTerminal) {
		t.Fatalf("expected ErrJobAlreadyTerminal when transitioning skipped -> success, got: %v", err)
	}

	// 3. Idempotent update to same terminal status succeeds
	if err := s.UpdateRunJobStatus(ctx, jobCancelled.ID, drevtypes.StatusCancelled); err != nil {
		t.Fatalf("idempotent update failed: %v", err)
	}
}

func TestPostgresStore_CancellationRacingWithCompletion_DeterministicPersistedState(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	runID := uuid.New().String()
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p", Status: drevtypes.StatusRunning})

	var startWg sync.WaitGroup
	startWg.Add(1)
	var doneWg sync.WaitGroup
	doneWg.Add(2)

	var errCancel, errSuccess error
	go func() {
		defer doneWg.Done()
		startWg.Wait()
		errCancel = s.UpdateRunStatus(ctx, runID, drevtypes.StatusCancelled)
	}()

	go func() {
		defer doneWg.Done()
		startWg.Wait()
		errSuccess = s.UpdateRunStatus(ctx, runID, drevtypes.StatusSuccess)
	}()

	startWg.Done()
	doneWg.Wait()

	if errCancel == nil && errSuccess == nil {
		t.Fatal("SAFETY VIOLATION: both cancellation and success updates succeeded concurrently!")
	}
	if errCancel != nil && errSuccess != nil {
		t.Fatalf("unexpected both updates failed: cancel=%v, success=%v", errCancel, errSuccess)
	}

	run, _ := s.GetRun(ctx, runID)
	if errCancel == nil {
		if run.Status != drevtypes.StatusCancelled {
			t.Errorf("cancel won but status is %s", run.Status)
		}
		if !errors.Is(errSuccess, ErrRunAlreadyTerminal) {
			t.Errorf("expected ErrRunAlreadyTerminal for success, got %v", errSuccess)
		}
	} else {
		if run.Status != drevtypes.StatusSuccess {
			t.Errorf("success won but status is %s", run.Status)
		}
		if !errors.Is(errCancel, ErrRunAlreadyTerminal) {
			t.Errorf("expected ErrRunAlreadyTerminal for cancel, got %v", errCancel)
		}
	}
}

func TestPostgresStore_FailRun_TerminalRunSemantics(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 1. Already-failed run: pending job becomes failed; terminal jobs remain unchanged
	runFailedID := uuid.New().String()
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runFailedID, PipelineID: "p", Status: drevtypes.StatusFailed})
	_ = s.UpdateRunStatus(ctx, runFailedID, drevtypes.StatusFailed)
	initialFailedRun, _ := s.GetRun(ctx, runFailedID)

	jobFPendingID := uuid.New().String()
	jobFSuccessID := uuid.New().String()
	jobFPending := &drevtypes.RunJob{ID: jobFPendingID, RunID: runFailedID, JobName: "pending-step", Status: drevtypes.StatusPending}
	jobFSuccess := &drevtypes.RunJob{ID: jobFSuccessID, RunID: runFailedID, JobName: "success-step", Status: drevtypes.StatusSuccess}
	_ = s.CreateRunJob(ctx, jobFPending)
	_ = s.CreateRunJob(ctx, jobFSuccess)

	if err := FailRun(ctx, s, runFailedID, nil); err != nil {
		t.Fatalf("FailRun on failed run returned error: %v", err)
	}

	runF, _ := s.GetRun(ctx, runFailedID)
	if runF.Status != drevtypes.StatusFailed {
		t.Errorf("run status changed: got %s, want failed", runF.Status)
	}
	if !runF.FinishedAt.Equal(initialFailedRun.FinishedAt) {
		t.Errorf("finished_at timestamp changed on FailRun: before %v, after %v", initialFailedRun.FinishedAt, runF.FinishedAt)
	}

	jobsF, _ := s.GetRunJobs(ctx, runFailedID)
	for _, j := range jobsF {
		if j.ID == jobFPendingID {
			if j.Status != drevtypes.StatusFailed {
				t.Errorf("expected pending job in failed run to become failed, got %s", j.Status)
			}
			if j.FinishedAt.IsZero() {
				t.Errorf("expected finished_at to be populated for finalized job")
			}
		} else if j.ID == jobFSuccessID {
			if j.Status != drevtypes.StatusSuccess {
				t.Errorf("expected successful job in failed run to remain success, got %s", j.Status)
			}
		}
	}

	// 2. Already-successful run: pending job remains pending; successful jobs remain unchanged
	runSuccessID := uuid.New().String()
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runSuccessID, PipelineID: "p", Status: drevtypes.StatusSuccess})
	_ = s.UpdateRunStatus(ctx, runSuccessID, drevtypes.StatusSuccess)
	initialSuccessRun, _ := s.GetRun(ctx, runSuccessID)

	jobSPendingID := uuid.New().String()
	jobSSuccessID := uuid.New().String()
	jobSPending := &drevtypes.RunJob{ID: jobSPendingID, RunID: runSuccessID, JobName: "pending-step", Status: drevtypes.StatusPending}
	jobSSuccess := &drevtypes.RunJob{ID: jobSSuccessID, RunID: runSuccessID, JobName: "success-step", Status: drevtypes.StatusSuccess}
	_ = s.CreateRunJob(ctx, jobSPending)
	_ = s.CreateRunJob(ctx, jobSSuccess)

	if err := FailRun(ctx, s, runSuccessID, nil); err != nil {
		t.Fatalf("FailRun on successful run returned error: %v", err)
	}

	runS, _ := s.GetRun(ctx, runSuccessID)
	if runS.Status != drevtypes.StatusSuccess {
		t.Errorf("run status changed: got %s, want success", runS.Status)
	}
	if !runS.FinishedAt.Equal(initialSuccessRun.FinishedAt) {
		t.Errorf("finished_at timestamp changed on FailRun: before %v, after %v", initialSuccessRun.FinishedAt, runS.FinishedAt)
	}

	jobsS, _ := s.GetRunJobs(ctx, runSuccessID)
	for _, j := range jobsS {
		if j.ID == jobSPendingID {
			if j.Status != drevtypes.StatusPending {
				t.Errorf("expected pending job in successful run to remain pending, got %s", j.Status)
			}
		} else if j.ID == jobSSuccessID {
			if j.Status != drevtypes.StatusSuccess {
				t.Errorf("expected successful job in successful run to remain success, got %s", j.Status)
			}
		}
	}

	// 3. Already-cancelled run: pending job remains pending; cancelled jobs remain unchanged
	runCancelledID := uuid.New().String()
	_ = s.CreateRun(ctx, &drevtypes.Run{ID: runCancelledID, PipelineID: "p", Status: drevtypes.StatusCancelled})
	_ = s.UpdateRunStatus(ctx, runCancelledID, drevtypes.StatusCancelled)
	initialCancelledRun, _ := s.GetRun(ctx, runCancelledID)

	jobCPendingID := uuid.New().String()
	jobCCancelledID := uuid.New().String()
	jobCPending := &drevtypes.RunJob{ID: jobCPendingID, RunID: runCancelledID, JobName: "pending-step", Status: drevtypes.StatusPending}
	jobCCancelled := &drevtypes.RunJob{ID: jobCCancelledID, RunID: runCancelledID, JobName: "cancelled-step", Status: drevtypes.StatusCancelled}
	_ = s.CreateRunJob(ctx, jobCPending)
	_ = s.CreateRunJob(ctx, jobCCancelled)

	if err := FailRun(ctx, s, runCancelledID, nil); err != nil {
		t.Fatalf("FailRun on cancelled run returned error: %v", err)
	}

	runC, _ := s.GetRun(ctx, runCancelledID)
	if runC.Status != drevtypes.StatusCancelled {
		t.Errorf("run status changed: got %s, want cancelled", runC.Status)
	}
	if !runC.FinishedAt.Equal(initialCancelledRun.FinishedAt) {
		t.Errorf("finished_at timestamp changed on FailRun: before %v, after %v", initialCancelledRun.FinishedAt, runC.FinishedAt)
	}

	jobsC, _ := s.GetRunJobs(ctx, runCancelledID)
	for _, j := range jobsC {
		if j.ID == jobCPendingID {
			if j.Status != drevtypes.StatusPending {
				t.Errorf("expected pending job in cancelled run to remain pending, got %s", j.Status)
			}
		} else if j.ID == jobCCancelledID {
			if j.Status != drevtypes.StatusCancelled {
				t.Errorf("expected cancelled job in cancelled run to remain cancelled, got %s", j.Status)
			}
		}
	}

	// 4. Repeated FailRun calls preserve run/job statuses and timestamps
	time.Sleep(10 * time.Millisecond)
	if err := FailRun(ctx, s, runFailedID, nil); err != nil {
		t.Fatalf("repeated FailRun on failed run failed: %v", err)
	}
	if err := FailRun(ctx, s, runSuccessID, nil); err != nil {
		t.Fatalf("repeated FailRun on success run failed: %v", err)
	}
	if err := FailRun(ctx, s, runCancelledID, nil); err != nil {
		t.Fatalf("repeated FailRun on cancelled run failed: %v", err)
	}

	repeatRunF, _ := s.GetRun(ctx, runFailedID)
	if !repeatRunF.FinishedAt.Equal(initialFailedRun.FinishedAt) {
		t.Errorf("repeat FailRun changed failed run finished_at: %v != %v", repeatRunF.FinishedAt, initialFailedRun.FinishedAt)
	}
	repeatRunS, _ := s.GetRun(ctx, runSuccessID)
	if !repeatRunS.FinishedAt.Equal(initialSuccessRun.FinishedAt) {
		t.Errorf("repeat FailRun changed success run finished_at: %v != %v", repeatRunS.FinishedAt, initialSuccessRun.FinishedAt)
	}
	repeatRunC, _ := s.GetRun(ctx, runCancelledID)
	if !repeatRunC.FinishedAt.Equal(initialCancelledRun.FinishedAt) {
		t.Errorf("repeat FailRun changed cancelled run finished_at: %v != %v", repeatRunC.FinishedAt, initialCancelledRun.FinishedAt)
	}
}

func TestPostgresStore_UpdateRunStatus_MissingRowCompatible(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	err := s.UpdateRunStatus(ctx, uuid.New().String(), drevtypes.StatusRunning)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for nonexistent run, got: %v", err)
	}

	err = s.UpdateRunJobStatus(ctx, uuid.New().String(), drevtypes.StatusRunning)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for nonexistent job, got: %v", err)
	}
}
