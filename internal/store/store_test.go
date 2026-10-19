package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
	_ "modernc.org/sqlite"
)

func TestOpen_FreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh.db")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Create a run
	run := &drevtypes.Run{
		ID:          "run-1",
		OrgID:       "org-demo",
		PipelineID:  "my-pipeline",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
		CommitSHA:   "abc1234",
		CommitMsg:   "initial commit",
		Branch:      "main",
		StartedAt:   time.Now().UTC().Truncate(time.Second),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	// 2. Create a run job
	job := &drevtypes.RunJob{
		ID:        "job-1",
		RunID:     run.ID,
		JobName:   "test",
		Status:    drevtypes.StatusPending,
		StartedAt: time.Now().UTC().Truncate(time.Second),
	}
	if err := s.CreateRunJob(ctx, job); err != nil {
		t.Fatalf("CreateRunJob() error = %v", err)
	}

	// 3. Query run
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if got.ID != run.ID || got.OrgID != run.OrgID || got.PipelineID != run.PipelineID || got.TriggeredBy != run.TriggeredBy {
		t.Errorf("GetRun() got = %+v, want %+v", got, run)
	}
	if len(got.Jobs) != 1 || got.Jobs[0].JobName != "test" {
		t.Errorf("GetRun() jobs = %+v, want 1 job 'test'", got.Jobs)
	}

	// 4. Update status
	if err := s.UpdateRunStatus(ctx, run.ID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("UpdateRunStatus() error = %v", err)
	}

	// 5. Test idempotency on repeated open
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() second time error = %v", err)
	}
	defer s2.Close()

	got2, err := s2.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun() after reopen error = %v", err)
	}
	if got2.Status != drevtypes.StatusSuccess {
		t.Errorf("GetRun() status = %v, want success", got2.Status)
	}
}

func TestOpen_LegacyDatabaseMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	// Step 1: Create an authentic older SQLite database with only the legacy schema
	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("creating legacy sqlite db: %v", err)
	}

	legacySchema := `
	CREATE TABLE runs (
		id TEXT PRIMARY KEY,
		pipeline_name TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME,
		finished_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE run_jobs (
		id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL REFERENCES runs(id),
		job_name TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME,
		finished_at DATETIME
	);`

	if _, err := legacyDB.Exec(legacySchema); err != nil {
		legacyDB.Close()
		t.Fatalf("executing legacy schema: %v", err)
	}

	// Step 2: Seed authentic legacy rows into the older database
	legacyRun1 := `INSERT INTO runs (id, pipeline_name, status, started_at, finished_at)
		VALUES ('legacy-run-1', 'pipeline-alpha', 'success', '2026-05-01T10:00:00Z', '2026-05-01T10:05:00Z');`
	legacyRun2 := `INSERT INTO runs (id, pipeline_name, status, started_at, finished_at)
		VALUES ('legacy-run-2', 'pipeline-beta', 'running', '2026-05-01T11:00:00Z', NULL);`
	legacyJob1 := `INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
		VALUES ('job-1', 'legacy-run-1', 'build', 'success', '2026-05-01T10:00:00Z', '2026-05-01T10:05:00Z');`

	for _, query := range []string{legacyRun1, legacyRun2, legacyJob1} {
		if _, err := legacyDB.Exec(query); err != nil {
			legacyDB.Close()
			t.Fatalf("inserting legacy seed data: %v", err)
		}
	}
	legacyDB.Close()

	// Step 3: Open the legacy database with store.Open() — this must run the migration
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() on legacy database failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Step 4: Verify legacy rows are preserved with expected column defaults
	r1, err := s.GetRun(ctx, "legacy-run-1")
	if err != nil {
		t.Fatalf("GetRun(legacy-run-1) error: %v", err)
	}
	if r1.ID != "legacy-run-1" {
		t.Errorf("r1.ID = %q, want 'legacy-run-1'", r1.ID)
	}
	if r1.PipelineID != "pipeline-alpha" {
		t.Errorf("r1.PipelineID = %q, want 'pipeline-alpha'", r1.PipelineID)
	}
	if r1.Status != drevtypes.StatusSuccess {
		t.Errorf("r1.Status = %q, want %q", r1.Status, drevtypes.StatusSuccess)
	}
	if r1.OrgID != "" {
		t.Errorf("r1.OrgID = %q, want '' (default)", r1.OrgID)
	}
	if r1.TriggeredBy != "api" {
		t.Errorf("r1.TriggeredBy = %q, want 'api' (default)", r1.TriggeredBy)
	}
	if r1.CommitSHA != "" || r1.CommitMsg != "" || r1.Branch != "" {
		t.Errorf("r1 commit info should be empty, got sha=%q msg=%q branch=%q", r1.CommitSHA, r1.CommitMsg, r1.Branch)
	}
	if len(r1.Jobs) != 1 || r1.Jobs[0].JobName != "build" {
		t.Errorf("r1 jobs = %+v, want 1 job 'build'", r1.Jobs)
	}

	r2, err := s.GetRun(ctx, "legacy-run-2")
	if err != nil {
		t.Fatalf("GetRun(legacy-run-2) error: %v", err)
	}
	if r2.Status != drevtypes.StatusRunning {
		t.Errorf("r2.Status = %q, want %q", r2.Status, drevtypes.StatusRunning)
	}
	if r2.OrgID != "" {
		t.Errorf("r2.OrgID = %q, want ''", r2.OrgID)
	}
	if r2.TriggeredBy != "api" {
		t.Errorf("r2.TriggeredBy = %q, want 'api'", r2.TriggeredBy)
	}

	// Step 5: Verify new reads and writes work normally on the upgraded database
	newRun := &drevtypes.Run{
		ID:          "new-run-3",
		OrgID:       "org-corp",
		PipelineID:  "pipeline-gamma",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
		CommitSHA:   "9915e5333a",
		CommitMsg:   "fix: sqlite migration",
		Branch:      "fix-branch",
		StartedAt:   time.Now().UTC().Truncate(time.Second),
	}
	if err := s.CreateRun(ctx, newRun); err != nil {
		t.Fatalf("CreateRun() on migrated database error: %v", err)
	}

	gotNew, err := s.GetRun(ctx, "new-run-3")
	if err != nil {
		t.Fatalf("GetRun(new-run-3) error: %v", err)
	}
	if gotNew.OrgID != "org-corp" || gotNew.TriggeredBy != "webhook" || gotNew.CommitSHA != "9915e5333a" || gotNew.Branch != "fix-branch" {
		t.Errorf("GetRun(new-run-3) got = %+v, want %+v", gotNew, newRun)
	}

	allRuns, err := s.ListRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListRuns() error: %v", err)
	}
	if len(allRuns) != 3 {
		t.Errorf("ListRuns() count = %d, want 3", len(allRuns))
	}

	// Step 6: Verify repeated opens do not fail or duplicate migration effects
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	sReopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("re-opening migrated database failed: %v", err)
	}
	defer sReopened.Close()

	allRunsAfterReopen, err := sReopened.ListRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListRuns() after reopen error: %v", err)
	}
	if len(allRunsAfterReopen) != 3 {
		t.Errorf("ListRuns() after reopen count = %d, want 3", len(allRunsAfterReopen))
	}
}

func TestOpen_LegacyDatabasePartialColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "partial.db")

	// Database that has org_id, but is missing triggered_by, commit_sha, commit_msg, branch
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("creating partial sqlite db: %v", err)
	}

	partialSchema := `
	CREATE TABLE runs (
		id TEXT PRIMARY KEY,
		org_id TEXT NOT NULL DEFAULT '',
		pipeline_name TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME,
		finished_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE run_jobs (
		id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL REFERENCES runs(id),
		job_name TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME,
		finished_at DATETIME
	);`

	if _, err := rawDB.Exec(partialSchema); err != nil {
		rawDB.Close()
		t.Fatalf("executing partial schema: %v", err)
	}

	if _, err := rawDB.Exec(`INSERT INTO runs (id, org_id, pipeline_name, status) VALUES ('p-1', 'org-p', 'pipe-p', 'success')`); err != nil {
		rawDB.Close()
		t.Fatalf("inserting seed data: %v", err)
	}
	rawDB.Close()

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() on partial database failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	r, err := s.GetRun(ctx, "p-1")
	if err != nil {
		t.Fatalf("GetRun(p-1) error: %v", err)
	}
	if r.OrgID != "org-p" {
		t.Errorf("r.OrgID = %q, want 'org-p'", r.OrgID)
	}
	if r.TriggeredBy != "api" {
		t.Errorf("r.TriggeredBy = %q, want 'api' (default)", r.TriggeredBy)
	}
}

func TestResetGhostRuns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Seed runs in various states
	runs := []*drevtypes.Run{
		{ID: "run-running", PipelineName: "pipe", Status: drevtypes.StatusRunning, StartedAt: time.Now()},
		{ID: "run-pending", PipelineName: "pipe", Status: drevtypes.StatusPending, StartedAt: time.Now()},
		{ID: "run-success", PipelineName: "pipe", Status: drevtypes.StatusSuccess, StartedAt: time.Now(), FinishedAt: time.Now()},
		{ID: "run-failed", PipelineName: "pipe", Status: drevtypes.StatusFailed, StartedAt: time.Now(), FinishedAt: time.Now()},
	}
	for _, r := range runs {
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatalf("CreateRun(%s) error = %v", r.ID, err)
		}
	}

	// Seed jobs
	jobs := []*drevtypes.RunJob{
		{ID: "j-running", RunID: "run-running", JobName: "test", Status: drevtypes.StatusRunning, StartedAt: time.Now()},
		{ID: "j-pending", RunID: "run-pending", JobName: "build", Status: drevtypes.StatusPending, StartedAt: time.Now()},
		{ID: "j-success", RunID: "run-success", JobName: "deploy", Status: drevtypes.StatusSuccess, StartedAt: time.Now(), FinishedAt: time.Now()},
	}
	for _, j := range jobs {
		if err := s.CreateRunJob(ctx, j); err != nil {
			t.Fatalf("CreateRunJob(%s) error = %v", j.ID, err)
		}
	}

	// First call to ResetGhostRuns: should identify and reset run-running and run-pending
	interrupted, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("ResetGhostRuns() error = %v", err)
	}

	if len(interrupted) != 2 {
		t.Fatalf("expected 2 interrupted runs, got %d (%v)", len(interrupted), interrupted)
	}

	interruptedMap := make(map[string]bool)
	for _, id := range interrupted {
		interruptedMap[id] = true
	}
	if !interruptedMap["run-running"] || !interruptedMap["run-pending"] {
		t.Errorf("expected run-running and run-pending to be returned, got: %v", interrupted)
	}

	// Verify states in store
	rRunning, _ := s.GetRun(ctx, "run-running")
	if rRunning.Status != drevtypes.StatusFailed {
		t.Errorf("run-running status = %s, want failed", rRunning.Status)
	}
	if rRunning.FinishedAt.IsZero() {
		t.Errorf("run-running finished_at was not set")
	}

	rPending, _ := s.GetRun(ctx, "run-pending")
	if rPending.Status != drevtypes.StatusFailed {
		t.Errorf("run-pending status = %s, want failed", rPending.Status)
	}

	// Verify success and failed runs were NOT mutated
	rSuccess, _ := s.GetRun(ctx, "run-success")
	if rSuccess.Status != drevtypes.StatusSuccess {
		t.Errorf("run-success status unexpectedly changed to %s", rSuccess.Status)
	}
	rFailed, _ := s.GetRun(ctx, "run-failed")
	if rFailed.Status != drevtypes.StatusFailed {
		t.Errorf("run-failed status unexpectedly changed to %s", rFailed.Status)
	}

	// Verify jobs were also updated
	runningJobs, _ := s.GetRunJobs(ctx, "run-running")
	if len(runningJobs) == 0 || runningJobs[0].Status != drevtypes.StatusFailed {
		t.Errorf("job j-running was not updated to failed")
	}

	// Second call to ResetGhostRuns: idempotent, should return nil
	interrupted2, err := s.ResetGhostRuns(ctx)
	if err != nil {
		t.Fatalf("second ResetGhostRuns() error = %v", err)
	}
	if len(interrupted2) != 0 {
		t.Errorf("expected 0 interrupted runs on repeated call, got: %v", interrupted2)
	}
}
