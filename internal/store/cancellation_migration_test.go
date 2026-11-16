package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func TestSQLite_FreshDatabase_HasCancellationRequestedAtColumn(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh_cancel.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open fresh store failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	cols, err := getExistingColumns(ctx, s.db, "runs")
	if err != nil {
		t.Fatalf("getExistingColumns failed: %v", err)
	}

	if !cols["cancellation_requested_at"] {
		t.Errorf("fresh database missing cancellation_requested_at column in runs table; cols = %v", cols)
	}
}

func TestSQLite_LegacyDatabase_AutoUpgrade_RetainsNull(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy_pre_cancel.db")

	// Step 1: Create a legacy database without cancellation_requested_at
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("opening raw sqlite: %v", err)
	}

	legacySchema := `
	CREATE TABLE runs (
		id TEXT PRIMARY KEY,
		org_id TEXT NOT NULL DEFAULT '',
		pipeline_name TEXT NOT NULL,
		status TEXT NOT NULL,
		triggered_by TEXT NOT NULL DEFAULT 'api',
		commit_sha TEXT,
		commit_msg TEXT,
		branch TEXT,
		started_at DATETIME,
		finished_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		failure_reason TEXT,
		interrupted_stage TEXT
	);
	CREATE TABLE run_jobs (
		id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL REFERENCES runs(id),
		job_name TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME,
		finished_at DATETIME,
		failure_reason TEXT
	);`

	if _, err := rawDB.Exec(legacySchema); err != nil {
		rawDB.Close()
		t.Fatalf("executing legacy schema: %v", err)
	}

	// Seed existing legacy runs
	seed1 := `INSERT INTO runs (id, org_id, pipeline_name, status, started_at, finished_at)
		VALUES ('legacy-run-1', 'org-1', 'pipeline-a', 'success', '2026-11-15T10:00:00Z', '2026-11-15T10:05:00Z');`
	seed2 := `INSERT INTO runs (id, org_id, pipeline_name, status, started_at, finished_at)
		VALUES ('legacy-run-2', 'org-1', 'pipeline-b', 'running', '2026-11-15T11:00:00Z', NULL);`

	for _, q := range []string{seed1, seed2} {
		if _, err := rawDB.Exec(q); err != nil {
			rawDB.Close()
			t.Fatalf("inserting seed data: %v", err)
		}
	}
	rawDB.Close()

	// Step 2: Open with store.Open() — triggers auto-migration
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open legacy db failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Verify column now exists
	cols, err := getExistingColumns(ctx, s.db, "runs")
	if err != nil {
		t.Fatalf("getExistingColumns failed: %v", err)
	}
	if !cols["cancellation_requested_at"] {
		t.Errorf("auto-migrated database missing cancellation_requested_at column")
	}

	// Step 3: Verify existing runs retain NULL cancellation timestamps
	r1, err := s.GetRun(ctx, "legacy-run-1")
	if err != nil {
		t.Fatalf("GetRun(legacy-run-1) failed: %v", err)
	}
	if r1.CancellationRequestedAt != nil {
		t.Errorf("expected r1.CancellationRequestedAt == nil, got %v", r1.CancellationRequestedAt)
	}
	if r1.Status != drevtypes.StatusSuccess {
		t.Errorf("expected r1.Status == success, got %s", r1.Status)
	}

	r2, err := s.GetRun(ctx, "legacy-run-2")
	if err != nil {
		t.Fatalf("GetRun(legacy-run-2) failed: %v", err)
	}
	if r2.CancellationRequestedAt != nil {
		t.Errorf("expected r2.CancellationRequestedAt == nil, got %v", r2.CancellationRequestedAt)
	}
	if r2.Status != drevtypes.StatusRunning {
		t.Errorf("expected r2.Status == running, got %s", r2.Status)
	}
}

func TestSQLite_PersistAndRead_PopulatedCancellationTimestamp(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "populated_cancel.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	cancelTime := time.Date(2026, 11, 15, 18, 45, 30, 0, time.UTC)
	runID := "run-with-cancel-timestamp"

	run := &drevtypes.Run{
		ID:                      runID,
		OrgID:                   "org-test",
		PipelineID:              "pipe-cancel",
		PipelineName:            "pipeline-cancel",
		Status:                  drevtypes.StatusRunning,
		TriggeredBy:             "api",
		StartedAt:               time.Date(2026, 11, 15, 18, 40, 0, 0, time.UTC),
		CancellationRequestedAt: &cancelTime,
	}

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	readBack, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}

	if readBack.CancellationRequestedAt == nil {
		t.Fatal("expected readBack.CancellationRequestedAt to be non-nil")
	}

	if !readBack.CancellationRequestedAt.Equal(cancelTime) {
		t.Errorf("expected %v, got %v", cancelTime, *readBack.CancellationRequestedAt)
	}
}

func TestSQLite_PersistAndRead_NullCancellationTimestamp(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "null_cancel.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-null-cancel"

	run := &drevtypes.Run{
		ID:                      runID,
		OrgID:                   "org-test",
		PipelineID:              "pipe-null",
		Status:                  drevtypes.StatusPending,
		TriggeredBy:             "api",
		CancellationRequestedAt: nil,
	}

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	readBack, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}

	if readBack.CancellationRequestedAt != nil {
		t.Errorf("expected nil CancellationRequestedAt, got %v", readBack.CancellationRequestedAt)
	}
}

func TestSQLite_PersistAndRead_SubsecondPrecisionAndUTCNormalization(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "subsecond_cancel.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Timestamp with non-zero sub-second precision (nanoseconds) and non-UTC timezone
	est := time.FixedZone("EST", -5*3600)
	inputTime := time.Date(2026, 11, 15, 14, 30, 45, 987654321, est)
	expectedUTC := inputTime.UTC()

	runID := "run-subsecond-cancel"
	run := &drevtypes.Run{
		ID:                      runID,
		OrgID:                   "org-subsecond",
		PipelineID:              "pipe-subsecond",
		Status:                  drevtypes.StatusRunning,
		TriggeredBy:             "api",
		CancellationRequestedAt: &inputTime,
	}

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	readBack, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}

	if readBack.CancellationRequestedAt == nil {
		t.Fatal("expected CancellationRequestedAt to be non-nil")
	}

	// Verify exact equality including nanoseconds
	if !readBack.CancellationRequestedAt.Equal(expectedUTC) {
		t.Errorf("expected Equal(%v), got %v", expectedUTC, *readBack.CancellationRequestedAt)
	}
	if readBack.CancellationRequestedAt.Nanosecond() != expectedUTC.Nanosecond() {
		t.Errorf("nanosecond mismatch: got %d, want %d", readBack.CancellationRequestedAt.Nanosecond(), expectedUTC.Nanosecond())
	}
	if readBack.CancellationRequestedAt.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", readBack.CancellationRequestedAt.Location())
	}

	// Also verify that CancelRun preserves subsecond precision on running run
	run2ID := "run-subsecond-cancelrun"
	if err := s.CreateRun(ctx, &drevtypes.Run{
		ID:         run2ID,
		PipelineID: "p2",
		Status:     drevtypes.StatusRunning,
	}); err != nil {
		t.Fatalf("CreateRun(run2) failed: %v", err)
	}

	cancelRes, err := s.CancelRun(ctx, run2ID, "")
	if err != nil {
		t.Fatalf("CancelRun failed: %v", err)
	}
	if cancelRes.Run.CancellationRequestedAt == nil {
		t.Fatal("expected cancelRes.Run.CancellationRequestedAt to be non-nil")
	}

	readRun2, err := s.GetRun(ctx, run2ID)
	if err != nil {
		t.Fatalf("GetRun(run2) failed: %v", err)
	}
	if readRun2.CancellationRequestedAt == nil {
		t.Fatal("expected readRun2.CancellationRequestedAt to be non-nil")
	}
	if !readRun2.CancellationRequestedAt.Equal(*cancelRes.Run.CancellationRequestedAt) {
		t.Errorf("persisted CancelRun timestamp mismatch: got %v, want %v", readRun2.CancellationRequestedAt, cancelRes.Run.CancellationRequestedAt)
	}
}

func TestPostgres_Migration007_FileSyntaxAndIdempotency(t *testing.T) {
	data, err := os.ReadFile("../../migrations/007_cancellation_intent.sql")
	if err != nil {
		t.Fatalf("reading migrations/007_cancellation_intent.sql failed: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "ALTER TABLE runs") {
		t.Errorf("migration 007 missing ALTER TABLE runs")
	}
	if !strings.Contains(content, "ADD COLUMN cancellation_requested_at TIMESTAMPTZ") {
		t.Errorf("migration 007 missing ADD COLUMN cancellation_requested_at TIMESTAMPTZ")
	}
	if !strings.Contains(content, "table_schema = CURRENT_SCHEMA") {
		t.Errorf("migration 007 missing CURRENT_SCHEMA check")
	}
	if !strings.Contains(content, "column_name = 'cancellation_requested_at'") {
		t.Errorf("migration 007 missing column_name check")
	}
	if !strings.Contains(content, "IF NOT EXISTS") {
		t.Errorf("migration 007 missing IF NOT EXISTS idempotency check")
	}
}
