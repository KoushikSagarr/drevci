package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	_ "modernc.org/sqlite"
)

// ErrNotSupported is returned by SQLite stubs for SaaS-only methods.
var ErrNotSupported = errors.New("operation not supported in single-tenant mode")

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	id            TEXT PRIMARY KEY,
	org_id        TEXT NOT NULL DEFAULT '',
	pipeline_name TEXT NOT NULL,
	status        TEXT NOT NULL,
	triggered_by  TEXT NOT NULL DEFAULT 'api',
	commit_sha    TEXT,
	commit_msg    TEXT,
	branch        TEXT,
	started_at    DATETIME,
	finished_at   DATETIME,
	created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS run_jobs (
	id          TEXT PRIMARY KEY,
	run_id      TEXT NOT NULL REFERENCES runs(id),
	job_name    TEXT NOT NULL,
	status      TEXT NOT NULL,
	started_at  DATETIME,
	finished_at DATETIME
);`

// Store defines the persistence interface for pipeline runs and SaaS tenancy.
type Store interface {
	// ── Core pipeline methods ──────────────────────────────────────────────
	CreateRun(ctx context.Context, run *drevtypes.Run) error
	GetRun(ctx context.Context, id string) (*drevtypes.Run, error)
	UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	ListRuns(ctx context.Context, limit int) ([]*drevtypes.Run, error)
	ListRunsByOrg(ctx context.Context, orgID string, limit int) ([]*drevtypes.Run, error)
	CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error
	UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	ResetGhostRuns(ctx context.Context) error
	GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error)

	// ── Org management (PostgreSQL / SaaS only) ───────────────────────────
	CreateOrg(ctx context.Context, org *drevtypes.Org) error
	GetOrgBySlug(ctx context.Context, slug string) (*drevtypes.Org, error)
	GetOrgByID(ctx context.Context, id string) (*drevtypes.Org, error)

	// ── Token management ──────────────────────────────────────────────────
	CreateToken(ctx context.Context, token *drevtypes.APIToken) error
	ValidateToken(ctx context.Context, tokenHash string) (*drevtypes.APIToken, error)
	GetOrgFromToken(ctx context.Context, tokenHash string) (*drevtypes.Org, error)

	// ── Usage tracking ────────────────────────────────────────────────────
	RecordUsage(ctx context.Context, orgID string, eventType string, runID string) error
	GetMonthlyUsage(ctx context.Context, orgID string) (int, error)
}

// ─── SQLite implementation ────────────────────────────────────────────────────

// SQLiteStore implements Store backed by a SQLite database.
type SQLiteStore struct {
	db                     *sql.DB
	stmtCreateRun          *sql.Stmt
	stmtGetRun             *sql.Stmt
	stmtUpdateRunStatus    *sql.Stmt
	stmtListRuns           *sql.Stmt
	stmtCreateRunJob       *sql.Stmt
	stmtUpdateRunJobStatus *sql.Stmt
	stmtGetRunJobs         *sql.Stmt
}

// Open creates or opens a SQLite database at dbPath, initialises
// the schema, enables WAL mode, and prepares all statements.
func Open(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL mode: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	s := &SQLiteStore{db: db}
	if err := s.prepare(); err != nil {
		db.Close()
		return nil, fmt.Errorf("preparing statements: %w", err)
	}

	return s, nil
}

// Close closes all prepared statements and the database connection.
func (s *SQLiteStore) Close() error {
	for _, stmt := range []*sql.Stmt{
		s.stmtCreateRun, s.stmtGetRun, s.stmtUpdateRunStatus,
		s.stmtListRuns, s.stmtCreateRunJob, s.stmtUpdateRunJobStatus,
		s.stmtGetRunJobs,
	} {
		if stmt != nil {
			stmt.Close()
		}
	}
	return s.db.Close()
}

func (s *SQLiteStore) prepare() error {
	var err error

	s.stmtCreateRun, err = s.db.Prepare(
		`INSERT INTO runs (id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtGetRun, err = s.db.Prepare(
		`SELECT id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at
		 FROM runs WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtUpdateRunStatus, err = s.db.Prepare(
		`UPDATE runs SET status = ?,
		 started_at  = CASE WHEN ? = 'running' AND started_at IS NULL THEN ? ELSE started_at END,
		 finished_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN ? ELSE finished_at END
		 WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtListRuns, err = s.db.Prepare(
		`SELECT id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at
		 FROM runs ORDER BY created_at DESC LIMIT ?`)
	if err != nil {
		return err
	}

	s.stmtCreateRunJob, err = s.db.Prepare(
		`INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtUpdateRunJobStatus, err = s.db.Prepare(
		`UPDATE run_jobs SET status = ?,
		 started_at  = CASE WHEN ? = 'running' AND started_at IS NULL THEN ? ELSE started_at END,
		 finished_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN ? ELSE finished_at END
		 WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtGetRunJobs, err = s.db.Prepare(
		`SELECT id, run_id, job_name, status, started_at, finished_at FROM run_jobs WHERE run_id = ?`)
	if err != nil {
		return err
	}

	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func nullTimeParam(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func scanTime(val *sql.NullString) time.Time {
	if !val.Valid {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, val.String)
	return t
}

func scanRun(row interface {
	Scan(...interface{}) error
}) (*drevtypes.Run, error) {
	var r drevtypes.Run
	var status, triggeredBy string
	var commitSHA, commitMsg, branch sql.NullString
	var startedAt, finishedAt sql.NullString

	err := row.Scan(
		&r.ID, &r.OrgID, &r.PipelineID, &status,
		&triggeredBy, &commitSHA, &commitMsg, &branch,
		&startedAt, &finishedAt,
	)
	if err != nil {
		return nil, err
	}
	r.Status = drevtypes.RunStatus(status)
	r.TriggeredBy = triggeredBy
	r.CommitSHA = commitSHA.String
	r.CommitMsg = commitMsg.String
	r.Branch = branch.String
	r.StartedAt = scanTime(&startedAt)
	r.FinishedAt = scanTime(&finishedAt)
	return &r, nil
}

// ─── Core methods ─────────────────────────────────────────────────────────────

func (s *SQLiteStore) CreateRun(ctx context.Context, run *drevtypes.Run) error {
	_, err := s.stmtCreateRun.ExecContext(ctx,
		run.ID, run.OrgID, run.PipelineID, string(run.Status),
		run.TriggeredBy, nullStr(run.CommitSHA), nullStr(run.CommitMsg), nullStr(run.Branch),
		nullTimeParam(run.StartedAt), nullTimeParam(run.FinishedAt),
	)
	return err
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func (s *SQLiteStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	r, err := scanRun(s.stmtGetRun.QueryRowContext(ctx, id))
	if err != nil {
		return nil, err
	}

	jobs, err := s.GetRunJobs(ctx, id)
	if err != nil {
		return nil, err
	}
	r.Jobs = make([]drevtypes.RunJob, len(jobs))
	for i, j := range jobs {
		r.Jobs[i] = *j
	}
	return r, nil
}

func (s *SQLiteStore) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.stmtUpdateRunStatus.ExecContext(ctx,
		string(status), string(status), now, string(status), now, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLiteStore) ListRuns(ctx context.Context, limit int) ([]*drevtypes.Run, error) {
	rows, err := s.stmtListRuns.QueryContext(ctx, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*drevtypes.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		jobs, err := s.GetRunJobs(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		r.Jobs = make([]drevtypes.RunJob, len(jobs))
		for i, j := range jobs {
			r.Jobs[i] = *j
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *SQLiteStore) ListRunsByOrg(ctx context.Context, orgID string, limit int) ([]*drevtypes.Run, error) {
	// SQLite single-tenant: just return all runs (org filtering not meaningful locally)
	return s.ListRuns(ctx, limit)
}

func (s *SQLiteStore) CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error {
	_, err := s.stmtCreateRunJob.ExecContext(ctx,
		job.ID, job.RunID, job.JobName, string(job.Status),
		nullTimeParam(job.StartedAt), nullTimeParam(job.FinishedAt),
	)
	return err
}

func (s *SQLiteStore) UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.stmtUpdateRunJobStatus.ExecContext(ctx,
		string(status), string(status), now, string(status), now, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLiteStore) GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error) {
	rows, err := s.stmtGetRunJobs.QueryContext(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*drevtypes.RunJob
	for rows.Next() {
		var j drevtypes.RunJob
		var status string
		var startedAt, finishedAt sql.NullString

		if err := rows.Scan(&j.ID, &j.RunID, &j.JobName, &status, &startedAt, &finishedAt); err != nil {
			return nil, err
		}
		j.Status = drevtypes.RunStatus(status)
		j.StartedAt = scanTime(&startedAt)
		j.FinishedAt = scanTime(&finishedAt)
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

func (s *SQLiteStore) ResetGhostRuns(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs SET status = 'failed', finished_at = ?
		WHERE status IN ('running', 'pending')`, now)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE run_jobs SET status = 'failed', finished_at = ?
		WHERE status IN ('running', 'pending')`, now)
	return err
}

// ─── SaaS stubs (not supported in SQLite mode) ───────────────────────────────

func (s *SQLiteStore) CreateOrg(_ context.Context, _ *drevtypes.Org) error {
	return ErrNotSupported
}

func (s *SQLiteStore) GetOrgBySlug(_ context.Context, _ string) (*drevtypes.Org, error) {
	return nil, ErrNotSupported
}

func (s *SQLiteStore) GetOrgByID(_ context.Context, _ string) (*drevtypes.Org, error) {
	return nil, ErrNotSupported
}

func (s *SQLiteStore) CreateToken(_ context.Context, _ *drevtypes.APIToken) error {
	return ErrNotSupported
}

func (s *SQLiteStore) ValidateToken(_ context.Context, _ string) (*drevtypes.APIToken, error) {
	return nil, ErrNotSupported
}

func (s *SQLiteStore) GetOrgFromToken(_ context.Context, _ string) (*drevtypes.Org, error) {
	return nil, ErrNotSupported
}

func (s *SQLiteStore) RecordUsage(_ context.Context, _, _, _ string) error {
	return nil // silently no-op in local mode
}

func (s *SQLiteStore) GetMonthlyUsage(_ context.Context, _ string) (int, error) {
	return 0, nil
}
