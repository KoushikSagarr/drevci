package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func TestSQLiteStore_Indexes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "indexes.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Verify required indexes exist in sqlite_master
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("re-opening sqlite db: %v", err)
	}
	defer db.Close()

	expectedIndexes := []string{
		"idx_runs_created_at",
		"idx_runs_status_created_at",
		"idx_run_jobs_run_id",
	}

	for _, idx := range expectedIndexes {
		var name string
		err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", idx).Scan(&name)
		if err != nil {
			t.Errorf("expected index %s to exist in sqlite_master, got err: %v", idx, err)
		}
	}

	// Verify query plan uses index for run_jobs by run_id
	var qpSelectID, qpOrder, qpFrom int
	var qpDetail string
	err = db.QueryRowContext(ctx, "EXPLAIN QUERY PLAN SELECT * FROM run_jobs WHERE run_id = ?", "test-run").Scan(&qpSelectID, &qpOrder, &qpFrom, &qpDetail)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	if !strings.Contains(qpDetail, "idx_run_jobs_run_id") {
		t.Errorf("expected query plan to use idx_run_jobs_run_id, got: %s", qpDetail)
	}
}

func TestSQLiteStore_GetRunStats_Empty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty_stats.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	stats, err := s.GetRunStats(context.Background(), "", 25)
	if err != nil {
		t.Fatalf("GetRunStats() error = %v", err)
	}

	if stats.CompletedSampleCount != 0 {
		t.Errorf("CompletedSampleCount = %d, want 0", stats.CompletedSampleCount)
	}
	if stats.DurationSampleCount != 0 {
		t.Errorf("DurationSampleCount = %d, want 0", stats.DurationSampleCount)
	}
	if stats.PassRate != nil {
		t.Errorf("PassRate = %v, want nil", *stats.PassRate)
	}
	if stats.MeanDurationSec != nil {
		t.Errorf("MeanDurationSec = %v, want nil", *stats.MeanDurationSec)
	}
	if stats.ActiveRunsCount != 0 {
		t.Errorf("ActiveRunsCount = %d, want 0", stats.ActiveRunsCount)
	}
	if stats.TotalRunsCount != 0 {
		t.Errorf("TotalRunsCount = %d, want 0", stats.TotalRunsCount)
	}
	if len(stats.RecentFailures) != 0 {
		t.Errorf("RecentFailures = %v, want empty", stats.RecentFailures)
	}
}

func TestSQLiteStore_GetRunStats_Mixed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mixed_stats.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	baseTime := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

	// 1. Success, 30s
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r1",
		PipelineID: "p1",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  baseTime,
		FinishedAt: baseTime.Add(30 * time.Second),
	})
	// 2. Success, 60s
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r2",
		PipelineID: "p1",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  baseTime.Add(1 * time.Minute),
		FinishedAt: baseTime.Add(2 * time.Minute),
	})
	// 3. Failed, 20s
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r3",
		PipelineID: "p1",
		Status:     drevtypes.StatusFailed,
		StartedAt:  baseTime.Add(3 * time.Minute),
		FinishedAt: baseTime.Add(3*time.Minute + 20*time.Second),
	})
	// 4. Cancelled, 10s
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r4",
		PipelineID: "p1",
		Status:     drevtypes.StatusCancelled,
		StartedAt:  baseTime.Add(4 * time.Minute),
		FinishedAt: baseTime.Add(4*time.Minute + 10*time.Second),
	})
	// 5. Running
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r5",
		PipelineID: "p1",
		Status:     drevtypes.StatusRunning,
		StartedAt:  baseTime.Add(5 * time.Minute),
	})
	// 6. Pending
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "r6",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	})

	stats, err := s.GetRunStats(ctx, "", 25)
	if err != nil {
		t.Fatalf("GetRunStats() error = %v", err)
	}

	if stats.TotalRunsCount != 6 {
		t.Errorf("TotalRunsCount = %d, want 6", stats.TotalRunsCount)
	}
	if stats.ActiveRunsCount != 2 {
		t.Errorf("ActiveRunsCount = %d, want 2", stats.ActiveRunsCount)
	}
	if stats.CompletedSampleCount != 4 {
		t.Errorf("CompletedSampleCount = %d, want 4", stats.CompletedSampleCount)
	}
	if stats.SucceededCount != 2 {
		t.Errorf("SucceededCount = %d, want 2", stats.SucceededCount)
	}
	if stats.FailedCount != 1 {
		t.Errorf("FailedCount = %d, want 1", stats.FailedCount)
	}
	if stats.CancelledCount != 1 {
		t.Errorf("CancelledCount = %d, want 1", stats.CancelledCount)
	}
	if stats.PassRate == nil || *stats.PassRate != 50 {
		t.Errorf("PassRate = %v, want 50", stats.PassRate)
	}
	// Total duration: 30 + 60 + 20 + 10 = 120s / 4 = 30s
	if stats.DurationSampleCount != 4 {
		t.Errorf("DurationSampleCount = %d, want 4", stats.DurationSampleCount)
	}
	if stats.MeanDurationSec == nil || *stats.MeanDurationSec != 30 {
		t.Errorf("MeanDurationSec = %v, want 30", stats.MeanDurationSec)
	}
	if len(stats.RecentFailures) != 1 || stats.RecentFailures[0].ID != "r3" {
		t.Errorf("RecentFailures = %+v, want r3", stats.RecentFailures)
	}
}

func TestSQLiteStore_GetRunStats_InvalidTimestamps(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "invalid_timestamps.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	baseTime := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

	// 1. Valid success: 40s
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "valid-s",
		PipelineID: "p1",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  baseTime,
		FinishedAt: baseTime.Add(40 * time.Second),
	})
	// 2. Failed with missing finish timestamp (e.g. runner crash)
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "crashed-f",
		PipelineID: "p1",
		Status:     drevtypes.StatusFailed,
		StartedAt:  baseTime.Add(1 * time.Minute),
	})
	// 3. Failed with inverted timestamp (finished before started)
	_ = s.CreateRun(ctx, &drevtypes.Run{
		ID:         "inverted-f",
		PipelineID: "p1",
		Status:     drevtypes.StatusFailed,
		StartedAt:  baseTime.Add(2 * time.Minute),
		FinishedAt: baseTime.Add(1 * time.Minute),
	})

	stats, err := s.GetRunStats(ctx, "", 25)
	if err != nil {
		t.Fatalf("GetRunStats() error = %v", err)
	}

	// Pass-rate denominator must include all 3 terminal runs
	if stats.CompletedSampleCount != 3 {
		t.Errorf("CompletedSampleCount = %d, want 3", stats.CompletedSampleCount)
	}
	if stats.SucceededCount != 1 || stats.FailedCount != 2 {
		t.Errorf("SucceededCount = %d, FailedCount = %d, want 1 and 2", stats.SucceededCount, stats.FailedCount)
	}
	// 1 / 3 = 33%
	if stats.PassRate == nil || *stats.PassRate != 33 {
		t.Errorf("PassRate = %v, want 33", stats.PassRate)
	}

	// Duration calculation must only average the 1 valid timed run
	if stats.DurationSampleCount != 1 {
		t.Errorf("DurationSampleCount = %d, want 1", stats.DurationSampleCount)
	}
	if stats.MeanDurationSec == nil || *stats.MeanDurationSec != 40 {
		t.Errorf("MeanDurationSec = %v, want 40", stats.MeanDurationSec)
	}

	// Recent failures must include both crashed and inverted failures
	if len(stats.RecentFailures) != 2 {
		t.Errorf("len(RecentFailures) = %d, want 2", len(stats.RecentFailures))
	}
}

func TestSQLiteStore_GetRunStats_SampleLimit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sample_limit.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	baseTime := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

	// Create 40 runs: oldest 10 failed, newest 30 success
	for i := 0; i < 40; i++ {
		st := drevtypes.StatusSuccess
		if i < 10 {
			st = drevtypes.StatusFailed
		}
		_ = s.CreateRun(ctx, &drevtypes.Run{
			ID:         fmt.Sprintf("run-%02d", i),
			PipelineID: "batch",
			Status:     st,
			StartedAt:  baseTime.Add(time.Duration(i) * time.Minute),
			FinishedAt: baseTime.Add(time.Duration(i)*time.Minute + 30*time.Second),
		})
	}

	// Default sample limit 25 evaluates the last 25 created runs (all success)
	stats25, err := s.GetRunStats(ctx, "", 25)
	if err != nil {
		t.Fatalf("GetRunStats(25) error = %v", err)
	}
	if stats25.CompletedSampleCount != 25 {
		t.Errorf("CompletedSampleCount = %d, want 25", stats25.CompletedSampleCount)
	}
	if stats25.PassRate == nil || *stats25.PassRate != 100 {
		t.Errorf("PassRate = %v, want 100", stats25.PassRate)
	}

	// Sample limit 35 takes the last 35 (30 success, 5 failed) -> 30/35 = 86%
	stats35, err := s.GetRunStats(ctx, "", 35)
	if err != nil {
		t.Fatalf("GetRunStats(35) error = %v", err)
	}
	if stats35.CompletedSampleCount != 35 {
		t.Errorf("CompletedSampleCount = %d, want 35", stats35.CompletedSampleCount)
	}
	if stats35.PassRate == nil || *stats35.PassRate != 86 {
		t.Errorf("PassRate = %v, want 86", stats35.PassRate)
	}
}

func TestUpdateRunJobStatus_Skipped(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	run := &drevtypes.Run{
		ID:           "run-skip-test",
		PipelineID:   "pipeline-1",
		PipelineName: "test-pipeline",
		Status:       drevtypes.StatusRunning,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	job := &drevtypes.RunJob{
		ID:      "job-to-skip",
		RunID:   run.ID,
		JobName: "downstream",
		Status:  drevtypes.StatusPending,
	}
	if err := s.CreateRunJob(ctx, job); err != nil {
		t.Fatalf("CreateRunJob() error = %v", err)
	}

	if err := s.UpdateRunJobStatus(ctx, job.ID, drevtypes.StatusSkipped); err != nil {
		t.Fatalf("UpdateRunJobStatus(StatusSkipped) error = %v", err)
	}

	jobs, err := s.GetRunJobs(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRunJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusSkipped {
		t.Errorf("job status = %v, want %v", jobs[0].Status, drevtypes.StatusSkipped)
	}
	if jobs[0].FinishedAt.IsZero() {
		t.Errorf("expected finished_at to be populated for skipped job, got %v", jobs[0].FinishedAt)
	}
	if !jobs[0].StartedAt.IsZero() {
		t.Errorf("expected started_at to be zero for skipped job, got %v", jobs[0].StartedAt)
	}
}

func TestStore_FailRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "failrun.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	run := &drevtypes.Run{
		ID:          "run-fail-test",
		PipelineID:  "pipe-fail-test",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	job1 := &drevtypes.RunJob{
		ID:      "job-fail-1",
		RunID:   run.ID,
		JobName: "job-1",
		Status:  drevtypes.StatusPending,
	}
	job2 := &drevtypes.RunJob{
		ID:      "job-fail-2",
		RunID:   run.ID,
		JobName: "job-2",
		Status:  drevtypes.StatusPending,
	}
	if err := s.CreateRunJob(ctx, job1); err != nil {
		t.Fatalf("CreateRunJob(1) error = %v", err)
	}
	if err := s.CreateRunJob(ctx, job2); err != nil {
		t.Fatalf("CreateRunJob(2) error = %v", err)
	}

	// 1. FailRun with explicit job IDs
	if err := store.FailRun(ctx, s, run.ID, []string{job1.ID, job2.ID}); err != nil {
		t.Fatalf("FailRun() error = %v", err)
	}

	r, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if r.Status != drevtypes.StatusFailed {
		t.Errorf("run status = %v, want %v", r.Status, drevtypes.StatusFailed)
	}
	if r.FinishedAt.IsZero() {
		t.Errorf("expected run finished_at to be populated")
	}

	jobs, err := s.GetRunJobs(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRunJobs() error = %v", err)
	}
	for _, j := range jobs {
		if j.Status != drevtypes.StatusFailed {
			t.Errorf("job %s status = %v, want %v", j.ID, j.Status, drevtypes.StatusFailed)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("expected job %s finished_at to be populated", j.ID)
		}
	}

	// 2. FailRun with empty jobIDs (discovery through GetRunJobs)
	run2 := &drevtypes.Run{
		ID:          "run-fail-disc",
		PipelineID:  "pipe-fail-disc",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "api",
	}
	_ = s.CreateRun(ctx, run2)
	job3 := &drevtypes.RunJob{
		ID:      "job-fail-3",
		RunID:   run2.ID,
		JobName: "job-3",
		Status:  drevtypes.StatusPending,
	}
	_ = s.CreateRunJob(ctx, job3)

	if err := store.FailRun(ctx, s, run2.ID, nil); err != nil {
		t.Fatalf("FailRun() with discovery error = %v", err)
	}
	r2, _ := s.GetRun(ctx, run2.ID)
	if r2.Status != drevtypes.StatusFailed {
		t.Errorf("discovered run status = %v, want %v", r2.Status, drevtypes.StatusFailed)
	}
	jobs2, _ := s.GetRunJobs(ctx, run2.ID)
	if len(jobs2) != 1 || jobs2[0].Status != drevtypes.StatusFailed {
		t.Errorf("discovered job status = %v, want %v", jobs2[0].Status, drevtypes.StatusFailed)
	}

	// 3. FailRun handles non-existent IDs gracefully by returning error and logging
	err = store.FailRun(ctx, s, "nonexistent-run", []string{"nonexistent-job"})
	if err == nil {
		t.Errorf("expected error for nonexistent run, got nil")
	}
}

func TestStore_WebhookDeliveryIdempotency(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "deliveries.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer s.Close()

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
		DeliveryID: "delivery-guid-12345",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   42,
		RunID:      "run-guid-67890",
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
	if rec.CreatedAt.IsZero() {
		t.Errorf("expected non-zero CreatedAt timestamp")
	}

	// 5. Attempting to record duplicate delivery returns ErrDuplicateDelivery
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
	if !strings.Contains(err.Error(), "duplicate webhook delivery") {
		t.Errorf("expected duplicate error, got: %v", err)
	}

	// 6. Persistence across db close and reopen
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store.Open() error = %v", err)
	}
	defer s2.Close()

	rec2, err := s2.GetDelivery(ctx, delivery.DeliveryID)
	if err != nil {
		t.Fatalf("GetDelivery() after reopen error = %v", err)
	}
	if rec2 == nil || rec2.DeliveryID != delivery.DeliveryID || rec2.RunID != delivery.RunID {
		t.Errorf("unexpected delivery after reopen: %+v", rec2)
	}
}

func TestStore_CreateRunWithDelivery_Atomicity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "atomic.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Successful atomic commit of delivery, run, and jobs
	del1 := &drevtypes.WebhookDelivery{
		DeliveryID: "del-atomic-001",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   10,
		RunID:      "run-atomic-001",
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
		{ID: "job-atomic-1", RunID: run1.ID, JobName: "build", Status: drevtypes.StatusPending},
		{ID: "job-atomic-2", RunID: run1.ID, JobName: "test", Status: drevtypes.StatusPending},
	}

	if err := s.CreateRunWithDelivery(ctx, del1, run1, jobs1, `{"name":"test-spec-1"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(1) error = %v", err)
	}

	// Verify all entities exist (delivery, run, jobs, and outbox)
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
		DeliveryID: "del-atomic-002",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   11,
		RunID:      "run-atomic-002",
	}
	run2 := &drevtypes.Run{
		ID:          del2.RunID,
		PipelineID:  "pipe-2",
		Status:      drevtypes.StatusPending,
		TriggeredBy: "webhook",
	}
	// Job 2 reuses the same primary key as Job 1, forcing failure on 2nd job insert
	failingJobs := []*drevtypes.RunJob{
		{ID: "job-atomic-fail-1", RunID: run2.ID, JobName: "step-1", Status: drevtypes.StatusPending},
		{ID: "job-atomic-fail-1", RunID: run2.ID, JobName: "step-2", Status: drevtypes.StatusPending},
	}

	err = s.CreateRunWithDelivery(ctx, del2, run2, failingJobs, `{"name":"test-spec-fail"}`)
	if err == nil {
		t.Fatalf("expected error on duplicate job ID, got nil")
	}

	// PROVE ROLLBACK: Neither delivery, run, partial job, nor outbox must exist in database!
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
		{ID: "job-atomic-valid-1", RunID: run2.ID, JobName: "step-1", Status: drevtypes.StatusPending},
		{ID: "job-atomic-valid-2", RunID: run2.ID, JobName: "step-2", Status: drevtypes.StatusPending},
	}
	if err := s.CreateRunWithDelivery(ctx, del2, run2, validJobs2, `{"name":"test-spec-2"}`); err != nil {
		t.Fatalf("retrying CreateRunWithDelivery with same delivery ID failed: %v", err)
	}
	retriedDel, _ := s.GetDelivery(ctx, del2.DeliveryID)
	if retriedDel == nil || retriedDel.RunID != run2.ID {
		t.Errorf("retried delivery not found or mismatched: %+v", retriedDel)
	}
	retriedRun, _ := s.GetRun(ctx, run2.ID)
	if retriedRun == nil {
		t.Errorf("retried run not found")
	}

	// 4. Duplicate delivery returns ErrDuplicateDelivery and creates no run
	delDup := &drevtypes.WebhookDelivery{
		DeliveryID: del2.DeliveryID,
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      "run-atomic-ghost",
	}
	runDup := &drevtypes.Run{
		ID:         "run-atomic-ghost",
		PipelineID: "pipe-ghost",
		Status:     drevtypes.StatusPending,
	}
	err = s.CreateRunWithDelivery(ctx, delDup, runDup, nil, "{}")
	if err == nil {
		t.Fatalf("expected ErrDuplicateDelivery on duplicate delivery ID, got nil")
	}
	if !errors.Is(err, store.ErrDuplicateDelivery) {
		t.Errorf("expected ErrDuplicateDelivery, got: %v", err)
	}
	ghostRun, _ := s.GetRun(ctx, "run-atomic-ghost")
	if ghostRun != nil {
		t.Errorf("ghost run should not have been created: %+v", ghostRun)
	}

	// 5. DeleteDelivery releases delivery claim allowing re-creation
	if err := s.DeleteDelivery(ctx, del2.DeliveryID); err != nil {
		t.Fatalf("DeleteDelivery failed: %v", err)
	}
	delDeleted, _ := s.GetDelivery(ctx, del2.DeliveryID)
	if delDeleted != nil {
		t.Errorf("expected delivery to be deleted, got: %+v", delDeleted)
	}
	runRecreated := &drevtypes.Run{
		ID:         "run-atomic-recreated",
		PipelineID: "pipe-recreated",
		Status:     drevtypes.StatusPending,
	}
	delRecreated := &drevtypes.WebhookDelivery{
		DeliveryID: del2.DeliveryID,
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runRecreated.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delRecreated, runRecreated, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery after DeleteDelivery failed: %v", err)
	}
	finalDel, _ := s.GetDelivery(ctx, del2.DeliveryID)
	if finalDel == nil || finalDel.RunID != runRecreated.ID {
		t.Errorf("recreated delivery not found: %+v", finalDel)
	}
}

func TestStore_ResetGhostRuns_ReleasesWebhookDeliveries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_deliveries.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Seed interrupted runs (pending, running) and terminal runs (success, failed)
	runPendingOutbox := &drevtypes.Run{
		ID:         "run-ghost-pending-outbox",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delPendingOutbox := &drevtypes.WebhookDelivery{
		DeliveryID: "del-ghost-pending-outbox",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runPendingOutbox.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delPendingOutbox, runPendingOutbox, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(pending-outbox): %v", err)
	}

	runPendingNoOutbox := &drevtypes.Run{
		ID:         "run-ghost-pending-no-outbox",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusPending,
	}
	delPendingNoOutbox := &drevtypes.WebhookDelivery{
		DeliveryID: "del-ghost-pending-no-outbox",
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
		ID:         "run-ghost-running",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusRunning,
	}
	delRunning := &drevtypes.WebhookDelivery{
		DeliveryID: "del-ghost-running",
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		RunID:      runRunning.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, delRunning, runRunning, nil, "{}"); err != nil {
		t.Fatalf("CreateRunWithDelivery(running): %v", err)
	}

	runSuccess := &drevtypes.Run{
		ID:         "run-ghost-success",
		PipelineID: "p-ghost",
		Status:     drevtypes.StatusSuccess,
	}
	delSuccess := &drevtypes.WebhookDelivery{
		DeliveryID: "del-ghost-success",
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
		t.Errorf("expected del-ghost-pending-no-outbox to be released, got: %+v", dNoOutbox)
	}
	dRunning, err := s.GetDelivery(ctx, delRunning.DeliveryID)
	if err != nil || dRunning != nil {
		t.Errorf("expected del-ghost-running to be released, got: %+v", dRunning)
	}

	// Delivery for surviving pending run with outbox is NOT released
	dOutbox, err := s.GetDelivery(ctx, delPendingOutbox.DeliveryID)
	if err != nil || dOutbox == nil {
		t.Errorf("expected del-ghost-pending-outbox to remain intact, got: %+v (err: %v)", dOutbox, err)
	}

	// 5. Verify webhook delivery records for SUCCESSFUL runs are NOT touched
	dSuccess, err := s.GetDelivery(ctx, delSuccess.DeliveryID)
	if err != nil || dSuccess == nil {
		t.Fatalf("expected del-ghost-success to be preserved, got nil (err: %v)", err)
	}
	if dSuccess.RunID != runSuccess.ID {
		t.Errorf("expected delSuccess.RunID == %q, got %q", runSuccess.ID, dSuccess.RunID)
	}

	// 6. Proves retry with same delivery ID succeeds after recovery
	runRetried := &drevtypes.Run{
		ID:         "run-retried-after-recovery",
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

func TestStore_ClaimRunForExecution_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "claim_success.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-claim-001",
		PipelineID: "pipe-claim",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-claim-001",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	spec := `{"name":"test-spec","jobs":[{"name":"j1"}]}`

	if err := s.CreateRunWithDelivery(ctx, del, run, nil, spec); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	// 1. Verify initially in pending outbox
	items, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(items) != 1 || items[0].RunID != run.ID {
		t.Fatalf("expected 1 pending outbox item, got: %+v (err: %v)", items, err)
	}
	if items[0].PipelineSpec != spec {
		t.Errorf("expected spec %q, got %q", spec, items[0].PipelineSpec)
	}

	// 2. Claim run for execution
	if err := s.ClaimRunForExecution(ctx, run.ID); err != nil {
		t.Fatalf("ClaimRunForExecution failed: %v", err)
	}

	// 3. Verify run status is running and started_at is persisted
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

	// 4. Verify outbox record was deleted in the claim transaction
	itemsAfter, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(itemsAfter) != 0 {
		t.Fatalf("expected outbox to be empty after claim, got: %+v", itemsAfter)
	}

	// 5. Verify delivery claim remains intact
	gotDel, err := s.GetDelivery(ctx, del.DeliveryID)
	if err != nil || gotDel == nil || gotDel.RunID != run.ID {
		t.Errorf("delivery claim should remain intact after claim: %+v", gotDel)
	}
}

func TestStore_ClaimRunForExecution_Concurrency(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "claim_concurrency.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-concurrent-claim",
		PipelineID: "pipe-claim",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-concurrent-claim",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, nil, `{"name":"concurrent"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	// Launch 10 concurrent worker goroutines attempting to claim the exact same run
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
			} else if errors.Is(err, store.ErrRunAlreadyClaimed) {
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

	// Confirm final run state
	finalRun, _ := s.GetRun(ctx, run.ID)
	if finalRun.Status != drevtypes.StatusRunning {
		t.Errorf("expected final status running, got %s", finalRun.Status)
	}
}

func TestStore_ClaimRunForExecution_Rejection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "claim_rejection.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Claiming non-existent run returns ErrRunAlreadyClaimed
	err = s.ClaimRunForExecution(ctx, "non-existent-run-id")
	if !errors.Is(err, store.ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for missing run, got: %v", err)
	}

	// 2. Claiming already running run returns ErrRunAlreadyClaimed
	runRunning := &drevtypes.Run{
		ID:         "run-already-running",
		PipelineID: "p1",
		Status:     drevtypes.StatusRunning,
	}
	if err := s.CreateRun(ctx, runRunning); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}
	err = s.ClaimRunForExecution(ctx, runRunning.ID)
	if !errors.Is(err, store.ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for running run, got: %v", err)
	}

	// 3. Claiming terminal success run returns ErrRunAlreadyClaimed
	runSuccess := &drevtypes.Run{
		ID:         "run-already-success",
		PipelineID: "p1",
		Status:     drevtypes.StatusSuccess,
	}
	if err := s.CreateRun(ctx, runSuccess); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}
	err = s.ClaimRunForExecution(ctx, runSuccess.ID)
	if !errors.Is(err, store.ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for success run, got: %v", err)
	}

	// 4. Claiming terminal failed run returns ErrRunAlreadyClaimed
	runFailed := &drevtypes.Run{
		ID:         "run-already-failed",
		PipelineID: "p1",
		Status:     drevtypes.StatusFailed,
	}
	if err := s.CreateRun(ctx, runFailed); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}
	err = s.ClaimRunForExecution(ctx, runFailed.ID)
	if !errors.Is(err, store.ErrRunAlreadyClaimed) {
		t.Errorf("expected ErrRunAlreadyClaimed for failed run, got: %v", err)
	}
}

func TestStore_GetPendingOutbox(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "get_pending_outbox.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Create 3 valid pending runs with outbox specs
	t1 := time.Now().UTC().Add(-30 * time.Minute)
	t2 := time.Now().UTC().Add(-20 * time.Minute)
	t3 := time.Now().UTC().Add(-10 * time.Minute)

	del1 := &drevtypes.WebhookDelivery{DeliveryID: "d1", RunID: "r1", CreatedAt: t1}
	del2 := &drevtypes.WebhookDelivery{DeliveryID: "d2", RunID: "r2", CreatedAt: t2}
	del3 := &drevtypes.WebhookDelivery{DeliveryID: "d3", RunID: "r3", CreatedAt: t3}

	run1 := &drevtypes.Run{ID: "r1", PipelineID: "p", Status: drevtypes.StatusPending}
	run2 := &drevtypes.Run{ID: "r2", PipelineID: "p", Status: drevtypes.StatusPending}
	run3 := &drevtypes.Run{ID: "r3", PipelineID: "p", Status: drevtypes.StatusPending}

	if err := s.CreateRunWithDelivery(ctx, del1, run1, nil, `{"id":"spec1"}`); err != nil {
		t.Fatalf("seed r1 failed: %v", err)
	}
	if err := s.CreateRunWithDelivery(ctx, del2, run2, nil, `{"id":"spec2"}`); err != nil {
		t.Fatalf("seed r2 failed: %v", err)
	}
	if err := s.CreateRunWithDelivery(ctx, del3, run3, nil, `{"id":"spec3"}`); err != nil {
		t.Fatalf("seed r3 failed: %v", err)
	}

	// 2. Claim r2 so it is running and should NOT appear in pending outbox
	if err := s.ClaimRunForExecution(ctx, "r2"); err != nil {
		t.Fatalf("claim r2 failed: %v", err)
	}

	// 3. Query pending outbox: should only return r1 and r3 in chronological order
	items, err := s.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].RunID != "r1" || items[1].RunID != "r3" {
		t.Errorf("expected [r1, r3] in chronological order, got: [%s, %s]", items[0].RunID, items[1].RunID)
	}

	// 4. Verify read-only: calling GetPendingOutbox again returns the same 2 items
	itemsSecond, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(itemsSecond) != 2 {
		t.Fatalf("GetPendingOutbox should not delete items on read, got %d items", len(itemsSecond))
	}

	// 5. Test limit enforcement
	itemsLimit, err := s.GetPendingOutbox(ctx, 1)
	if err != nil || len(itemsLimit) != 1 || itemsLimit[0].RunID != "r1" {
		t.Fatalf("expected limit 1 to return r1, got: %+v", itemsLimit)
	}
}

func TestStore_FailRunWithDelivery_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fail_run_delivery.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-fail-test-01",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-fail-test-01",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	jobs := []*drevtypes.RunJob{
		{ID: "j1", RunID: run.ID, JobName: "build", Status: drevtypes.StatusPending},
		{ID: "j2", RunID: run.ID, JobName: "test", Status: drevtypes.StatusPending},
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, jobs, `{"name":"fail-spec"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	// Atomic fail run, jobs, outbox deletion, and delivery claim release
	if err := s.FailRunWithDelivery(ctx, run.ID, []string{"j1", "j2"}, del.DeliveryID); err != nil {
		t.Fatalf("FailRunWithDelivery failed: %v", err)
	}

	// Verify run is failed with terminal timestamp
	r, err := s.GetRun(ctx, run.ID)
	if err != nil || r == nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if r.Status != drevtypes.StatusFailed || r.FinishedAt.IsZero() {
		t.Errorf("expected run failed with finished_at, got: %+v", r)
	}

	// Verify jobs are failed with terminal timestamps
	gotJobs, err := s.GetRunJobs(ctx, run.ID)
	if err != nil || len(gotJobs) != 2 {
		t.Fatalf("GetRunJobs failed: %v", err)
	}
	for _, j := range gotJobs {
		if j.Status != drevtypes.StatusFailed || j.FinishedAt.IsZero() {
			t.Errorf("expected job failed with finished_at, got: %+v", j)
		}
	}

	// Verify outbox entry is deleted
	outboxItems, err := s.GetPendingOutbox(ctx, 10)
	if err != nil || len(outboxItems) != 0 {
		t.Errorf("expected outbox entry to be deleted, found: %+v", outboxItems)
	}

	// Verify webhook delivery claim is deleted (released for retry)
	delCheck, err := s.GetDelivery(ctx, del.DeliveryID)
	if err != nil || delCheck != nil {
		t.Errorf("expected delivery claim to be deleted, found: %+v", delCheck)
	}
}

func TestStore_FailRunWithDelivery_Rollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fail_rollback.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	run := &drevtypes.Run{
		ID:         "run-rollback-test-01",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del := &drevtypes.WebhookDelivery{
		DeliveryID: "del-rollback-test-01",
		EventType:  "push",
		Repo:       "octocat/hello-world",
		RunID:      run.ID,
	}
	if err := s.CreateRunWithDelivery(ctx, del, run, nil, `{"name":"rollback-spec"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery error = %v", err)
	}

	// Attempt FailRunWithDelivery on a non-existent run ID -> should fail and roll back
	err = s.FailRunWithDelivery(ctx, "non-existent-run-id", nil, del.DeliveryID)
	if err == nil {
		t.Fatalf("expected error on non-existent run, got nil")
	}

	// PROVE ROLLBACK: The original delivery claim for del-rollback-test-01 must STILL EXIST!
	delCheck, err := s.GetDelivery(ctx, del.DeliveryID)
	if err != nil || delCheck == nil {
		t.Fatalf("delivery claim must NOT be deleted when FailRunWithDelivery fails: %+v", delCheck)
	}
	if delCheck.RunID != run.ID {
		t.Errorf("expected delivery to point to original run %s, got %s", run.ID, delCheck.RunID)
	}

	// Original run must still be pending
	runCheck, _ := s.GetRun(ctx, run.ID)
	if runCheck.Status != drevtypes.StatusPending {
		t.Errorf("expected run to remain pending, got: %s", runCheck.Status)
	}
}

func TestStore_CreateRunWithDelivery_EmptyPipelineSpecRejected(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty_spec.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Completely empty string
	run1 := &drevtypes.Run{ID: "run-empty-1", PipelineID: "p1", Status: drevtypes.StatusPending}
	del1 := &drevtypes.WebhookDelivery{DeliveryID: "del-empty-1", RunID: run1.ID}
	err = s.CreateRunWithDelivery(ctx, del1, run1, nil, "")
	if err == nil {
		t.Fatalf("expected error for empty pipelineSpec, got nil")
	}

	// Prove nothing committed
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

	// 2. Whitespace-only string
	run2 := &drevtypes.Run{ID: "run-empty-2", PipelineID: "p1", Status: drevtypes.StatusPending}
	del2 := &drevtypes.WebhookDelivery{DeliveryID: "del-empty-2", RunID: run2.ID}
	err = s.CreateRunWithDelivery(ctx, del2, run2, nil, "   \t\n  ")
	if err == nil {
		t.Fatalf("expected error for whitespace pipelineSpec, got nil")
	}

	// 3. Retry with valid non-empty specification succeeds
	err = s.CreateRunWithDelivery(ctx, del1, run1, nil, `{"name":"valid-pipeline"}`)
	if err != nil {
		t.Fatalf("CreateRunWithDelivery with valid spec failed: %v", err)
	}
	dValid, _ := s.GetDelivery(ctx, del1.DeliveryID)
	if dValid == nil || dValid.RunID != run1.ID {
		t.Errorf("expected valid delivery to be persisted: %+v", dValid)
	}
}

func TestStore_FailRunWithDelivery_ScopedToRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fail_scoped.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Seed Run A and Delivery A
	runA := &drevtypes.Run{ID: "run-A", PipelineID: "pA", Status: drevtypes.StatusPending}
	delA := &drevtypes.WebhookDelivery{DeliveryID: "del-A", RunID: runA.ID}
	jobsA := []*drevtypes.RunJob{{ID: "job-A", RunID: runA.ID, JobName: "build", Status: drevtypes.StatusPending}}
	if err := s.CreateRunWithDelivery(ctx, delA, runA, jobsA, `{"name":"specA"}`); err != nil {
		t.Fatalf("seed run A failed: %v", err)
	}

	// Seed Run B and Delivery B
	runB := &drevtypes.Run{ID: "run-B", PipelineID: "pB", Status: drevtypes.StatusPending}
	delB := &drevtypes.WebhookDelivery{DeliveryID: "del-B", RunID: runB.ID}
	jobsB := []*drevtypes.RunJob{{ID: "job-B", RunID: runB.ID, JobName: "build", Status: drevtypes.StatusPending}}
	if err := s.CreateRunWithDelivery(ctx, delB, runB, jobsB, `{"name":"specB"}`); err != nil {
		t.Fatalf("seed run B failed: %v", err)
	}

	// Case 1: Pass mismatched job ID (job-B belongs to Run B, not Run A)
	// Must fail and roll back all changes, ensuring no partial cleanup or delivery deletion
	err = s.FailRunWithDelivery(ctx, runA.ID, []string{"job-B"}, delA.DeliveryID)
	if err == nil {
		t.Fatalf("expected error when passing mismatched job ID, got nil")
	}
	if !strings.Contains(err.Error(), "run job job-B not found for run run-A") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify rollback: Run A must still be pending, Job A pending, Delivery A intact
	runACheck, _ := s.GetRun(ctx, runA.ID)
	if runACheck.Status != drevtypes.StatusPending {
		t.Errorf("run A must remain pending after rollback, got %s", runACheck.Status)
	}
	delACheck, _ := s.GetDelivery(ctx, delA.DeliveryID)
	if delACheck == nil {
		t.Fatalf("delivery A must NOT be deleted when job validation fails")
	}

	// Case 2: Pass mismatched delivery ID (del-B belongs to Run B, not Run A)
	err = s.FailRunWithDelivery(ctx, runA.ID, []string{"job-A"}, delB.DeliveryID)
	if err == nil {
		t.Fatalf("expected error when passing mismatched delivery ID, got nil")
	}
	if !strings.Contains(err.Error(), "webhook delivery del-B not found for run run-A") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify Run B and Delivery B are completely untouched
	delBCheck, err := s.GetDelivery(ctx, delB.DeliveryID)
	if err != nil || delBCheck == nil {
		t.Fatalf("delivery B must NOT be deleted: %+v (err: %v)", delBCheck, err)
	}
	jobsBCheck, _ := s.GetRunJobs(ctx, runB.ID)
	if len(jobsBCheck) != 1 || jobsBCheck[0].Status != drevtypes.StatusPending {
		t.Errorf("job B should still be pending, got: %+v", jobsBCheck[0])
	}
	runBCheck, _ := s.GetRun(ctx, runB.ID)
	if runBCheck.Status != drevtypes.StatusPending {
		t.Errorf("run B should still be pending, got: %s", runBCheck.Status)
	}

	// Case 3: Correctly scoped cleanup for Run A
	if err := s.FailRunWithDelivery(ctx, runA.ID, []string{"job-A"}, delA.DeliveryID); err != nil {
		t.Fatalf("valid FailRunWithDelivery failed: %v", err)
	}

	// Run A is now failed, Job A is failed, Delivery A is deleted
	runACheck, _ = s.GetRun(ctx, runA.ID)
	if runACheck.Status != drevtypes.StatusFailed {
		t.Errorf("run A should be failed, got: %s", runACheck.Status)
	}
	jobsACheck, _ := s.GetRunJobs(ctx, runA.ID)
	if len(jobsACheck) != 1 || jobsACheck[0].Status != drevtypes.StatusFailed {
		t.Errorf("job A should be failed, got: %+v", jobsACheck[0])
	}
	delACheck, _ = s.GetDelivery(ctx, delA.DeliveryID)
	if delACheck != nil {
		t.Errorf("delivery A should be deleted after successful FailRunWithDelivery")
	}

	// Run B is STILL unaffected
	runBCheck, _ = s.GetRun(ctx, runB.ID)
	if runBCheck.Status != drevtypes.StatusPending {
		t.Errorf("run B should still be pending, got: %s", runBCheck.Status)
	}
}

func TestSQLiteStore_CreateRunJobs_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "batch_success.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Seed run
	runID := "run-batch-001"
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: "pipe-batch-1",
		Status:     drevtypes.StatusRunning,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	jobs := []*drevtypes.RunJob{
		{
			ID:         "job-b-1",
			RunID:      runID,
			JobName:    "build",
			Status:     drevtypes.StatusRunning,
			StartedAt:  now,
			FinishedAt: now.Add(time.Minute),
		},
		{
			ID:      "job-b-2",
			RunID:   runID,
			JobName: "test",
			Status:  drevtypes.StatusPending,
		},
		{
			ID:      "job-b-3",
			RunID:   runID,
			JobName: "deploy",
			Status:  drevtypes.StatusPending,
		},
	}

	if err := s.CreateRunJobs(ctx, runID, jobs); err != nil {
		t.Fatalf("CreateRunJobs: %v", err)
	}

	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(gotJobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(gotJobs))
	}

	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range gotJobs {
		jobMap[j.ID] = j
	}

	// Verify job 1 timestamps and status
	j1, ok := jobMap["job-b-1"]
	if !ok {
		t.Fatalf("job-b-1 not found in store")
	}
	if j1.JobName != "build" || j1.Status != drevtypes.StatusRunning {
		t.Errorf("job-b-1 mismatch: %+v", j1)
	}
	if !j1.StartedAt.Equal(now) {
		t.Errorf("expected StartedAt %v, got %v", now, j1.StartedAt)
	}
	if !j1.FinishedAt.Equal(now.Add(time.Minute)) {
		t.Errorf("expected FinishedAt %v, got %v", now.Add(time.Minute), j1.FinishedAt)
	}

	// Verify job 2 and 3
	j2, ok := jobMap["job-b-2"]
	if !ok || j2.JobName != "test" || j2.Status != drevtypes.StatusPending {
		t.Errorf("job-b-2 mismatch: %+v", j2)
	}
	j3, ok := jobMap["job-b-3"]
	if !ok || j3.JobName != "deploy" || j3.Status != drevtypes.StatusPending {
		t.Errorf("job-b-3 mismatch: %+v", j3)
	}
}

func TestSQLiteStore_CreateRunJobs_DefaultPendingStatus(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "batch_default_status.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	runID := "run-batch-def"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p1", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	jobs := []*drevtypes.RunJob{
		{
			ID:      "job-empty-status",
			RunID:   runID,
			JobName: "lint",
			Status:  "", // Empty status should default to StatusPending
		},
	}

	if err := s.CreateRunJobs(ctx, runID, jobs); err != nil {
		t.Fatalf("CreateRunJobs: %v", err)
	}

	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil || len(gotJobs) != 1 {
		t.Fatalf("expected 1 job, got %d (err: %v)", len(gotJobs), err)
	}
	if gotJobs[0].Status != drevtypes.StatusPending {
		t.Errorf("expected default status StatusPending, got %s", gotJobs[0].Status)
	}
}

func TestSQLiteStore_CreateRunJobs_ValidationErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "batch_validation.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	runID := "run-valid-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p1", Status: drevtypes.StatusPending}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	cases := []struct {
		name        string
		runID       string
		jobs        []*drevtypes.RunJob
		errContains string
	}{
		{
			name:        "blank runID empty string",
			runID:       "",
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: "", JobName: "build"}},
			errContains: "runID cannot be empty",
		},
		{
			name:        "blank runID whitespace only",
			runID:       "   ",
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: "   ", JobName: "build"}},
			errContains: "runID cannot be empty",
		},
		{
			name:        "nil jobs slice",
			runID:       runID,
			jobs:        nil,
			errContains: "jobs slice cannot be nil",
		},
		{
			name:        "empty jobs slice",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{},
			errContains: "jobs slice cannot be empty",
		},
		{
			name:        "nil job entry at index 0",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{nil},
			errContains: "cannot be nil",
		},
		{
			name:        "nil job entry at index 1",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: runID, JobName: "b"}, nil},
			errContains: "cannot be nil",
		},
		{
			name:        "blank job ID",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{{ID: "  ", RunID: runID, JobName: "b"}},
			errContains: "blank ID",
		},
		{
			name:        "blank job name",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: runID, JobName: "  "}},
			errContains: "blank JobName",
		},
		{
			name:        "mismatched job RunID",
			runID:       runID,
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: "different-run-id", JobName: "b"}},
			errContains: "mismatched RunID",
		},
		{
			name:  "duplicate job ID within batch",
			runID: runID,
			jobs: []*drevtypes.RunJob{
				{ID: "dup-id", RunID: runID, JobName: "build"},
				{ID: "dup-id", RunID: runID, JobName: "test"},
			},
			errContains: "duplicate job ID",
		},
		{
			name:  "duplicate job name within batch",
			runID: runID,
			jobs: []*drevtypes.RunJob{
				{ID: "j1", RunID: runID, JobName: "same-name"},
				{ID: "j2", RunID: runID, JobName: "same-name"},
			},
			errContains: "duplicate job name",
		},
		{
			name:        "nonexistent run ID",
			runID:       "run-does-not-exist",
			jobs:        []*drevtypes.RunJob{{ID: "j1", RunID: "run-does-not-exist", JobName: "build"}},
			errContains: "not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.CreateRunJobs(ctx, tc.runID, tc.jobs)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("expected error containing %q, got: %v", tc.errContains, err)
			}
		})
	}

	// Verify valid run still has 0 jobs
	gotJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(gotJobs) != 0 {
		t.Errorf("expected 0 jobs in valid run after validation failures, got %d", len(gotJobs))
	}
}

func TestSQLiteStore_CreateRunJobs_Atomicity_RollbackOnConstraintFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "batch_rollback.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	runID := "run-rb-atomic-1"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p-rb", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// 1. Seed an existing job into the database
	seededJobID := "job-seeded-001"
	if err := s.CreateRunJob(ctx, &drevtypes.RunJob{
		ID:      seededJobID,
		RunID:   runID,
		JobName: "setup",
		Status:  drevtypes.StatusSuccess,
	}); err != nil {
		t.Fatalf("CreateRunJob(seeded): %v", err)
	}

	// Verify seeded job exists
	initialJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil || len(initialJobs) != 1 {
		t.Fatalf("expected 1 seeded job, got %d (err: %v)", len(initialJobs), err)
	}

	// 2. Submit a batch whose first job has a NEW ID and second job reuses seededJobID
	// Pre-validation passes because within the batch IDs and names are unique!
	newJobID := "job-new-transient-001"
	failingBatch := []*drevtypes.RunJob{
		{
			ID:      newJobID,
			RunID:   runID,
			JobName: "build-step",
			Status:  drevtypes.StatusPending,
		},
		{
			ID:      seededJobID, // Reuses existing ID in DB -> causes PK constraint failure in DB
			RunID:   runID,
			JobName: "test-step",
			Status:  drevtypes.StatusPending,
		},
	}

	err = s.CreateRunJobs(ctx, runID, failingBatch)
	if err == nil {
		t.Fatal("expected database constraint error on duplicate job ID, got nil")
	}

	// 3. PROVE ROLLBACK: newJobID must NOT exist in the database!
	afterRollbackJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs after rollback: %v", err)
	}
	if len(afterRollbackJobs) != 1 {
		t.Fatalf("expected exactly 1 job remaining after rollback, found %d", len(afterRollbackJobs))
	}
	if afterRollbackJobs[0].ID != seededJobID {
		t.Errorf("expected only seeded job %s, found %s", seededJobID, afterRollbackJobs[0].ID)
	}

	// Direct query via separate connection to verify job-new-transient-001 is completely absent
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer rawDB.Close()

	var count int
	row := rawDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_jobs WHERE id = ?`, newJobID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("querying for rolled-back job: %v", err)
	}
	if count != 0 {
		t.Errorf("job %s was not rolled back! found in database (count=%d)", newJobID, count)
	}

	// 4. Retry with valid jobs succeeds after rollback
	validBatch := []*drevtypes.RunJob{
		{
			ID:      newJobID,
			RunID:   runID,
			JobName: "build-step",
			Status:  drevtypes.StatusPending,
		},
		{
			ID:      "job-new-valid-002",
			RunID:   runID,
			JobName: "test-step",
			Status:  drevtypes.StatusPending,
		},
	}

	if err := s.CreateRunJobs(ctx, runID, validBatch); err != nil {
		t.Fatalf("retry CreateRunJobs failed: %v", err)
	}

	finalJobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs after retry: %v", err)
	}
	if len(finalJobs) != 3 {
		t.Fatalf("expected 3 total jobs after retry, found %d", len(finalJobs))
	}
}

func TestSQLiteStore_CreateRunJob_Compatible(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "single_job_compat.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	runID := "run-single-compat"
	if err := s.CreateRun(ctx, &drevtypes.Run{ID: runID, PipelineID: "p-compat", Status: drevtypes.StatusRunning}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	job := &drevtypes.RunJob{
		ID:      "single-j1",
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
	if gotJobs[0].ID != "single-j1" || gotJobs[0].JobName != "compile" {
		t.Errorf("job mismatch: %+v", gotJobs[0])
	}
}

func TestStore_ResetGhostRuns_OutboxAwareness(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ghost_outbox_awareness.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Pending run WITH outbox entry
	runPendingWithOutbox := &drevtypes.Run{
		ID:         "run-pending-with-outbox",
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}
	del1 := &drevtypes.WebhookDelivery{DeliveryID: "del-1", RunID: runPendingWithOutbox.ID}
	if err := s.CreateRunWithDelivery(ctx, del1, runPendingWithOutbox, nil, `{"name":"p1"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(pending-with-outbox): %v", err)
	}

	// 2. Pending run WITHOUT outbox entry (e.g. legacy/API trigger)
	runPendingNoOutbox := &drevtypes.Run{
		ID:         "run-pending-no-outbox",
		PipelineID: "p2",
		Status:     drevtypes.StatusPending,
	}
	if err := s.CreateRun(ctx, runPendingNoOutbox); err != nil {
		t.Fatalf("CreateRun(pending-no-outbox): %v", err)
	}

	// 3. Running run WITH outbox entry (e.g. claim interrupted before outbox delete)
	runRunningWithOutbox := &drevtypes.Run{
		ID:         "run-running-with-outbox",
		PipelineID: "p3",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now(),
	}
	del3 := &drevtypes.WebhookDelivery{DeliveryID: "del-3", RunID: runRunningWithOutbox.ID}
	if err := s.CreateRunWithDelivery(ctx, del3, runRunningWithOutbox, nil, `{"name":"p3"}`); err != nil {
		t.Fatalf("CreateRunWithDelivery(running-with-outbox): %v", err)
	}

	// 4. Running run WITHOUT outbox entry (standard claimed executing run)
	runRunningNoOutbox := &drevtypes.Run{
		ID:         "run-running-no-outbox",
		PipelineID: "p4",
		Status:     drevtypes.StatusRunning,
		StartedAt:  time.Now(),
	}
	if err := s.CreateRun(ctx, runRunningNoOutbox); err != nil {
		t.Fatalf("CreateRun(running-no-outbox): %v", err)
	}

	// 5. Terminal runs: success, failed, cancelled
	runSuccess := &drevtypes.Run{
		ID:         "run-terminal-success",
		PipelineID: "p5",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  time.Now().Add(-10 * time.Minute),
		FinishedAt: time.Now().Add(-5 * time.Minute),
	}
	runFailed := &drevtypes.Run{
		ID:         "run-terminal-failed",
		PipelineID: "p6",
		Status:     drevtypes.StatusFailed,
		StartedAt:  time.Now().Add(-10 * time.Minute),
		FinishedAt: time.Now().Add(-5 * time.Minute),
	}
	runCancelled := &drevtypes.Run{
		ID:         "run-terminal-cancelled",
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

	if interruptedSet["run-pending-with-outbox"] {
		t.Errorf("INVARIANT VIOLATION: pending run with outbox was marked as interrupted")
	}
	if !interruptedSet["run-pending-no-outbox"] {
		t.Errorf("expected pending run without outbox to be interrupted")
	}
	if !interruptedSet["run-running-with-outbox"] {
		t.Errorf("expected running run with outbox to be interrupted")
	}
	if !interruptedSet["run-running-no-outbox"] {
		t.Errorf("expected running run without outbox to be interrupted")
	}

	// Verify DB statuses
	r1, _ := s.GetRun(ctx, "run-pending-with-outbox")
	if r1.Status != drevtypes.StatusPending {
		t.Errorf("expected run-pending-with-outbox to remain pending, got: %s", r1.Status)
	}

	r2, _ := s.GetRun(ctx, "run-pending-no-outbox")
	if r2.Status != drevtypes.StatusFailed || r2.FinishedAt.IsZero() {
		t.Errorf("expected run-pending-no-outbox to be failed with finished_at, got: %+v", r2)
	}

	r3, _ := s.GetRun(ctx, "run-running-with-outbox")
	if r3.Status != drevtypes.StatusFailed || r3.FinishedAt.IsZero() {
		t.Errorf("expected run-running-with-outbox to be failed with finished_at, got: %+v", r3)
	}

	r4, _ := s.GetRun(ctx, "run-running-no-outbox")
	if r4.Status != drevtypes.StatusFailed || r4.FinishedAt.IsZero() {
		t.Errorf("expected run-running-no-outbox to be failed with finished_at, got: %+v", r4)
	}

	// Terminal runs must remain unchanged
	rSucc, _ := s.GetRun(ctx, "run-terminal-success")
	if rSucc.Status != drevtypes.StatusSuccess {
		t.Errorf("expected run-terminal-success to remain success, got: %s", rSucc.Status)
	}
	rFail, _ := s.GetRun(ctx, "run-terminal-failed")
	if rFail.Status != drevtypes.StatusFailed {
		t.Errorf("expected run-terminal-failed to remain failed, got: %s", rFail.Status)
	}
	rCanc, _ := s.GetRun(ctx, "run-terminal-cancelled")
	if rCanc.Status != drevtypes.StatusCancelled {
		t.Errorf("expected run-terminal-cancelled to remain cancelled, got: %s", rCanc.Status)
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
	if !outboxRunIDs["run-pending-with-outbox"] {
		t.Errorf("outbox entry for run-pending-with-outbox was unexpectedly removed")
	}
}
