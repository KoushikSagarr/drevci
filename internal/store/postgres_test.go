//go:build postgres

package store

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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
