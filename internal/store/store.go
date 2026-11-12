package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
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
);

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status_created_at ON runs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_run_jobs_run_id ON run_jobs(run_id);`

// ErrDuplicateDelivery indicates that a webhook delivery with the same ID has already been recorded.
var ErrDuplicateDelivery = errors.New("duplicate webhook delivery")

// ErrRunAlreadyClaimed indicates that a run is no longer pending or has already been claimed by another worker.
var ErrRunAlreadyClaimed = errors.New("run already claimed or not pending")

// Store defines the persistence interface for pipeline runs and SaaS tenancy.
type Store interface {
	// ── Core pipeline methods ──────────────────────────────────────────────
	CreateRun(ctx context.Context, run *drevtypes.Run) error
	GetRun(ctx context.Context, id string) (*drevtypes.Run, error)
	UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	ListRuns(ctx context.Context, limit int) ([]*drevtypes.Run, error)
	ListRunsByOrg(ctx context.Context, orgID string, limit int) ([]*drevtypes.Run, error)
	CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error
	CreateRunJobs(ctx context.Context, runID string, jobs []*drevtypes.RunJob) error
	UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	ResetGhostRuns(ctx context.Context) ([]string, error)
	GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error)
	GetRunStats(ctx context.Context, orgID string, sampleLimit int) (*drevtypes.RunStats, error)

	// ── Webhook delivery idempotency & durable outbox ─────────────────────
	CreateRunWithDelivery(ctx context.Context, delivery *drevtypes.WebhookDelivery, run *drevtypes.Run, jobs []*drevtypes.RunJob, pipelineSpec string) error
	RecordDelivery(ctx context.Context, delivery *drevtypes.WebhookDelivery) error
	GetDelivery(ctx context.Context, deliveryID string) (*drevtypes.WebhookDelivery, error)
	DeleteDelivery(ctx context.Context, deliveryID string) error
	ClaimRunForExecution(ctx context.Context, runID string) error
	FailRunWithDelivery(ctx context.Context, runID string, jobIDs []string, deliveryID string) error
	GetPendingOutbox(ctx context.Context, limit int) ([]*drevtypes.OutboxItem, error)

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
	stmtGetTerminalRuns    *sql.Stmt
	stmtGetRecentFailures  *sql.Stmt
	stmtCountActiveRuns    *sql.Stmt
	stmtCountTotalRuns     *sql.Stmt
	stmtRecordDelivery     *sql.Stmt
	stmtGetDelivery        *sql.Stmt
}

type columnDef struct {
	name       string
	definition string
}

var requiredRunsColumns = []columnDef{
	{name: "org_id", definition: "TEXT NOT NULL DEFAULT ''"},
	{name: "triggered_by", definition: "TEXT NOT NULL DEFAULT 'api'"},
	{name: "commit_sha", definition: "TEXT"},
	{name: "commit_msg", definition: "TEXT"},
	{name: "branch", definition: "TEXT"},
	{name: "started_at", definition: "DATETIME"},
	{name: "finished_at", definition: "DATETIME"},
	{name: "created_at", definition: "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"},
}

var requiredRunJobsColumns = []columnDef{
	{name: "started_at", definition: "DATETIME"},
	{name: "finished_at", definition: "DATETIME"},
}

func getExistingColumns(ctx context.Context, db *sql.DB, tableName string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, fmt.Errorf("querying table_info for %s: %w", tableName, err)
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scanning table_info for %s: %w", tableName, err)
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

func migrateTable(ctx context.Context, db *sql.DB, tableName string, requiredCols []columnDef) error {
	existingCols, err := getExistingColumns(ctx, db, tableName)
	if err != nil {
		return err
	}

	var missing []columnDef
	for _, col := range requiredCols {
		if !existingCols[col.name] {
			missing = append(missing, col)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning migration transaction for %s: %w", tableName, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, col := range missing {
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, col.name, col.definition)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("executing %q: %w", stmt, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing migration transaction for %s: %w", tableName, err)
	}

	return nil
}

func migrateSQLite(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("creating base schema: %w", err)
	}

	if err := migrateTable(ctx, db, "runs", requiredRunsColumns); err != nil {
		return fmt.Errorf("migrating runs table: %w", err)
	}

	if err := migrateTable(ctx, db, "run_jobs", requiredRunJobsColumns); err != nil {
		return fmt.Errorf("migrating run_jobs table: %w", err)
	}

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS webhook_deliveries (
		delivery_id TEXT PRIMARY KEY,
		event_type  TEXT NOT NULL,
		repo        TEXT NOT NULL DEFAULT '',
		pr_number   INTEGER NOT NULL DEFAULT 0,
		run_id      TEXT NOT NULL,
		created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("creating webhook_deliveries table: %w", err)
	}

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS pipeline_outbox (
		run_id        TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
		pipeline_spec TEXT NOT NULL,
		created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("creating pipeline_outbox table: %w", err)
	}

	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_status_created_at ON runs(status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_run_jobs_run_id ON run_jobs(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_run_id ON webhook_deliveries(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_pipeline_outbox_created_at ON pipeline_outbox(created_at ASC)`,
	}
	for _, idx := range indexes {
		if _, err := db.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("creating index: %w", err)
		}
	}

	return nil
}

// Open creates or opens a SQLite database at dbPath, initialises
// the schema, enables WAL mode, runs safe migrations, and prepares all statements.
func Open(dbPath string) (*SQLiteStore, error) {
	dsn := dbPath
	if !strings.Contains(dsn, "?") {
		dsn += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	} else if !strings.Contains(dsn, "busy_timeout") {
		dsn += "&_pragma=busy_timeout(5000)"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL mode: %w", err)
	}

	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("setting busy timeout: %w", err)
	}

	if err := migrateSQLite(context.Background(), db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
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
		s.stmtGetRunJobs, s.stmtGetTerminalRuns, s.stmtGetRecentFailures,
		s.stmtCountActiveRuns, s.stmtCountTotalRuns,
		s.stmtRecordDelivery, s.stmtGetDelivery,
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
		 FROM runs ORDER BY created_at DESC, rowid DESC LIMIT ?`)
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
		 finished_at = CASE WHEN ? IN ('success', 'failed', 'cancelled', 'skipped') THEN ? ELSE finished_at END
		 WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtGetRunJobs, err = s.db.Prepare(
		`SELECT id, run_id, job_name, status, started_at, finished_at FROM run_jobs WHERE run_id = ?`)
	if err != nil {
		return err
	}

	s.stmtGetTerminalRuns, err = s.db.Prepare(
		`SELECT id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at
		 FROM runs
		 WHERE status IN ('success', 'failed', 'cancelled')
		 ORDER BY created_at DESC, rowid DESC
		 LIMIT ?`)
	if err != nil {
		return err
	}

	s.stmtGetRecentFailures, err = s.db.Prepare(
		`SELECT id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at
		 FROM runs
		 WHERE status = 'failed'
		 ORDER BY created_at DESC, rowid DESC
		 LIMIT 5`)
	if err != nil {
		return err
	}

	s.stmtCountActiveRuns, err = s.db.Prepare(
		`SELECT COUNT(*) FROM runs WHERE status IN ('running', 'pending')`)
	if err != nil {
		return err
	}

	s.stmtCountTotalRuns, err = s.db.Prepare(
		`SELECT COUNT(*) FROM runs`)
	if err != nil {
		return err
	}

	s.stmtRecordDelivery, err = s.db.Prepare(
		`INSERT INTO webhook_deliveries (delivery_id, event_type, repo, pr_number, run_id)
		 VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtGetDelivery, err = s.db.Prepare(
		`SELECT delivery_id, event_type, repo, pr_number, run_id, created_at
		 FROM webhook_deliveries WHERE delivery_id = ?`)
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

// validateRunJobsBatch validates the batch parameters for CreateRunJobs.
func validateRunJobsBatch(runID string, jobs []*drevtypes.RunJob) error {
	if strings.TrimSpace(runID) == "" {
		return errors.New("runID cannot be empty")
	}
	if jobs == nil {
		return errors.New("jobs slice cannot be nil")
	}
	if len(jobs) == 0 {
		return errors.New("jobs slice cannot be empty")
	}

	seenIDs := make(map[string]bool, len(jobs))
	seenNames := make(map[string]bool, len(jobs))

	for i, job := range jobs {
		if job == nil {
			return fmt.Errorf("job at index %d cannot be nil", i)
		}
		if strings.TrimSpace(job.ID) == "" {
			return fmt.Errorf("job at index %d has blank ID", i)
		}
		if strings.TrimSpace(job.JobName) == "" {
			return fmt.Errorf("job at index %d has blank JobName", i)
		}
		if job.RunID != runID {
			return fmt.Errorf("job %s has mismatched RunID: expected %s, got %s", job.ID, runID, job.RunID)
		}
		if seenIDs[job.ID] {
			return fmt.Errorf("duplicate job ID %s in batch", job.ID)
		}
		seenIDs[job.ID] = true

		if seenNames[job.JobName] {
			return fmt.Errorf("duplicate job name %s in batch", job.JobName)
		}
		seenNames[job.JobName] = true
	}

	return nil
}

// CreateRunJobs atomically inserts a batch of jobs for a run in a single transaction.
// If any job insertion or validation fails, zero newly inserted jobs are committed.
func (s *SQLiteStore) CreateRunJobs(ctx context.Context, runID string, jobs []*drevtypes.RunJob) error {
	if err := validateRunJobsBatch(runID, jobs); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning sqlite transaction: %w", err)
	}
	defer tx.Rollback()

	// Verify target run exists
	var exists int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM runs WHERE id = ?`, runID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("run %s not found: %w", runID, sql.ErrNoRows)
		}
		return fmt.Errorf("verifying run %s: %w", runID, err)
	}

	for _, job := range jobs {
		status := job.Status
		if status == "" {
			status = drevtypes.StatusPending
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			job.ID, runID, job.JobName, string(status),
			nullTimeParam(job.StartedAt), nullTimeParam(job.FinishedAt),
		)
		if err != nil {
			return fmt.Errorf("inserting run job %s: %w", job.JobName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing sqlite transaction: %w", err)
	}
	return nil
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

func (s *SQLiteStore) ResetGhostRuns(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM runs WHERE status IN ('running', 'pending')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("beginning reset ghost runs transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET status = 'failed', finished_at = ?
		WHERE status IN ('running', 'pending')`, now); err != nil {
		return nil, fmt.Errorf("updating ghost runs: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE run_jobs SET status = 'failed', finished_at = ?
		WHERE status IN ('running', 'pending')`, now); err != nil {
		return nil, fmt.Errorf("updating ghost run jobs: %w", err)
	}

	// Release webhook delivery claims for interrupted runs so that GitHub redeliveries can be accepted
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE run_id = ?`, id); err != nil {
			return nil, fmt.Errorf("releasing webhook delivery for ghost run %s: %w", id, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing reset ghost runs: %w", err)
	}

	return ids, nil
}

func (s *SQLiteStore) GetRunStats(ctx context.Context, _ string, sampleLimit int) (*drevtypes.RunStats, error) {
	if sampleLimit <= 0 {
		sampleLimit = 25
	}
	if sampleLimit > 100 {
		sampleLimit = 100
	}

	// 1. Terminal runs for the sample
	rows, err := s.stmtGetTerminalRuns.QueryContext(ctx, sampleLimit)
	if err != nil {
		return nil, fmt.Errorf("querying terminal runs: %w", err)
	}
	defer rows.Close()

	var (
		succeededCount      int
		failedCount         int
		cancelledCount      int
		durationSampleCount int
		totalDurationSec    float64
	)

	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning terminal run: %w", err)
		}

		switch r.Status {
		case drevtypes.StatusSuccess:
			succeededCount++
		case drevtypes.StatusFailed:
			failedCount++
		case drevtypes.StatusCancelled:
			cancelledCount++
		}

		// Valid non-negative duration check
		if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() && (r.FinishedAt.Equal(r.StartedAt) || r.FinishedAt.After(r.StartedAt)) {
			durationSampleCount++
			totalDurationSec += r.FinishedAt.Sub(r.StartedAt).Seconds()
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading terminal runs: %w", err)
	}

	completedSampleCount := succeededCount + failedCount + cancelledCount

	var passRate *int
	if completedSampleCount > 0 {
		pr := int(math.Round(float64(succeededCount) / float64(completedSampleCount) * 100.0))
		passRate = &pr
	}

	var meanDurationSec *int
	if durationSampleCount > 0 {
		avg := int(math.Round(totalDurationSec / float64(durationSampleCount)))
		meanDurationSec = &avg
	}

	// 2. Recent failures across recent history (up to 5)
	failRows, err := s.stmtGetRecentFailures.QueryContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying recent failures: %w", err)
	}
	defer failRows.Close()

	recentFailures := make([]*drevtypes.Run, 0)
	for failRows.Next() {
		r, err := scanRun(failRows)
		if err != nil {
			return nil, fmt.Errorf("scanning recent failure: %w", err)
		}
		recentFailures = append(recentFailures, r)
	}
	if err := failRows.Err(); err != nil {
		return nil, fmt.Errorf("reading recent failures: %w", err)
	}

	// 3. Active runs count (running or pending)
	var activeRunsCount int
	if err := s.stmtCountActiveRuns.QueryRowContext(ctx).Scan(&activeRunsCount); err != nil {
		return nil, fmt.Errorf("counting active runs: %w", err)
	}

	// 4. Total runs count
	var totalRunsCount int
	if err := s.stmtCountTotalRuns.QueryRowContext(ctx).Scan(&totalRunsCount); err != nil {
		return nil, fmt.Errorf("counting total runs: %w", err)
	}

	return &drevtypes.RunStats{
		SampleLimit:          sampleLimit,
		CompletedSampleCount: completedSampleCount,
		DurationSampleCount:  durationSampleCount,
		SucceededCount:       succeededCount,
		FailedCount:          failedCount,
		CancelledCount:       cancelledCount,
		PassRate:             passRate,
		MeanDurationSec:      meanDurationSec,
		RecentFailures:       recentFailures,
		ActiveRunsCount:      activeRunsCount,
		TotalRunsCount:       totalRunsCount,
	}, nil
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

// CreateRunWithDelivery atomically creates a webhook delivery record, a run, all associated run jobs,
// and a pipeline outbox specification record in a single transaction.
// Either all records commit or none commit. Returns ErrDuplicateDelivery on duplicate delivery ID.
func (s *SQLiteStore) CreateRunWithDelivery(
	ctx context.Context,
	delivery *drevtypes.WebhookDelivery,
	run *drevtypes.Run,
	jobs []*drevtypes.RunJob,
	pipelineSpec string,
) error {
	if delivery == nil || delivery.DeliveryID == "" {
		return errors.New("delivery cannot be nil or empty")
	}
	if run == nil || run.ID == "" {
		return errors.New("run cannot be nil or empty")
	}
	if strings.TrimSpace(pipelineSpec) == "" {
		return errors.New("pipelineSpec cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning sqlite transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Insert webhook delivery record
	now := time.Now().UTC().Format(time.RFC3339)
	if !delivery.CreatedAt.IsZero() {
		now = delivery.CreatedAt.UTC().Format(time.RFC3339)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO webhook_deliveries (delivery_id, event_type, repo, pr_number, run_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		delivery.DeliveryID, delivery.EventType, delivery.Repo, delivery.PRNumber, delivery.RunID, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "constraint failed") {
			return ErrDuplicateDelivery
		}
		return fmt.Errorf("inserting webhook delivery: %w", err)
	}

	// 2. Insert run record
	_, err = tx.ExecContext(ctx,
		`INSERT INTO runs (id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.OrgID, run.PipelineID, string(run.Status),
		run.TriggeredBy, nullStr(run.CommitSHA), nullStr(run.CommitMsg), nullStr(run.Branch),
		nullTimeParam(run.StartedAt), nullTimeParam(run.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("inserting run: %w", err)
	}

	// 3. Insert all associated run jobs
	for _, job := range jobs {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			job.ID, job.RunID, job.JobName, string(job.Status),
			nullTimeParam(job.StartedAt), nullTimeParam(job.FinishedAt),
		)
		if err != nil {
			return fmt.Errorf("inserting run job %s: %w", job.JobName, err)
		}
	}

	// 4. Insert into pipeline_outbox
	_, err = tx.ExecContext(ctx,
		`INSERT INTO pipeline_outbox (run_id, pipeline_spec, created_at)
		 VALUES (?, ?, ?)`,
		run.ID, pipelineSpec, now,
	)
	if err != nil {
		return fmt.Errorf("inserting pipeline outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing sqlite transaction: %w", err)
	}
	return nil
}

// ClaimRunForExecution atomically claims a pending run for execution.
// It verifies the run is currently pending with started_at IS NULL, transitions it to running,
// sets started_at, and deletes the pipeline_outbox entry in a single transaction.
// Returns ErrRunAlreadyClaimed if the run was already claimed, running, or finished.
func (s *SQLiteStore) ClaimRunForExecution(ctx context.Context, runID string) error {
	if runID == "" {
		return errors.New("runID cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning claim transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'running', started_at = ?
		WHERE id = ? AND status = 'pending' AND started_at IS NULL`,
		now, runID,
	)
	if err != nil {
		return fmt.Errorf("updating run for claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected on claim: %w", err)
	}
	if n == 0 {
		return ErrRunAlreadyClaimed
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_outbox WHERE run_id = ?`, runID); err != nil {
		return fmt.Errorf("deleting outbox entry for claimed run %s: %w", runID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing claim transaction: %w", err)
	}
	return nil
}

// GetPendingOutbox retrieves up to limit pending, never-started runs from the outbox,
// ordered deterministically by creation time.
func (s *SQLiteStore) GetPendingOutbox(ctx context.Context, limit int) ([]*drevtypes.OutboxItem, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT o.run_id, o.pipeline_spec, o.created_at
		FROM pipeline_outbox o
		JOIN runs r ON r.id = o.run_id
		WHERE r.status = 'pending' AND r.started_at IS NULL
		ORDER BY o.created_at ASC, o.rowid ASC
		LIMIT ?`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying pending outbox: %w", err)
	}
	defer rows.Close()

	var items []*drevtypes.OutboxItem
	for rows.Next() {
		var item drevtypes.OutboxItem
		var createdAt sql.NullString
		if err := rows.Scan(&item.RunID, &item.PipelineSpec, &createdAt); err != nil {
			return nil, fmt.Errorf("scanning outbox item: %w", err)
		}
		item.CreatedAt = scanTime(&createdAt)
		items = append(items, &item)
	}
	return items, rows.Err()
}

// FailRunWithDelivery atomically marks a run and its jobs as failed with terminal timestamps,
// deletes any matching pipeline_outbox record, and deletes the matching webhook delivery claim.
// Any failure rolls back all changes.
func (s *SQLiteStore) FailRunWithDelivery(ctx context.Context, runID string, jobIDs []string, deliveryID string) error {
	if runID == "" {
		return errors.New("runID cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning fail run with delivery transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Mark run failed
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'failed',
		    finished_at = CASE WHEN finished_at IS NULL THEN ? ELSE finished_at END
		WHERE id = ?`, now, runID,
	)
	if err != nil {
		return fmt.Errorf("updating run %s to failed: %w", runID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected on run failure: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("run %s not found", runID)
	}

	// 2. Mark relevant jobs failed
	if len(jobIDs) > 0 {
		for _, jid := range jobIDs {
			res, err := tx.ExecContext(ctx, `
				UPDATE run_jobs
				SET status = 'failed',
				    finished_at = CASE WHEN finished_at IS NULL THEN ? ELSE finished_at END
				WHERE id = ? AND run_id = ?`, now, jid, runID)
			if err != nil {
				return fmt.Errorf("updating run job %s to failed: %w", jid, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("checking rows affected for job %s: %w", jid, err)
			}
			if n == 0 {
				return fmt.Errorf("run job %s not found for run %s", jid, runID)
			}
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE run_jobs
			SET status = 'failed',
			    finished_at = CASE WHEN finished_at IS NULL THEN ? ELSE finished_at END
			WHERE run_id = ?`, now, runID); err != nil {
			return fmt.Errorf("updating run jobs for run %s to failed: %w", runID, err)
		}
	}

	// 3. Delete matching outbox record
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_outbox WHERE run_id = ?`, runID); err != nil {
		return fmt.Errorf("deleting outbox record for run %s: %w", runID, err)
	}

	// 4. Delete matching webhook delivery claim if deliveryID is provided (scoped strictly to this run)
	if deliveryID != "" {
		res, err := tx.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE delivery_id = ? AND run_id = ?`, deliveryID, runID)
		if err != nil {
			return fmt.Errorf("deleting delivery claim %s for run %s: %w", deliveryID, runID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("checking rows affected on delivery deletion: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("webhook delivery %s not found for run %s", deliveryID, runID)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing fail run with delivery transaction: %w", err)
	}
	return nil
}

// DeleteDelivery removes a webhook delivery record, used to release delivery claims when enqueue is rejected.
func (s *SQLiteStore) DeleteDelivery(ctx context.Context, deliveryID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE delivery_id = ?`, deliveryID)
	return err
}

// RecordDelivery persists a webhook delivery record. Returns ErrDuplicateDelivery if the delivery ID already exists.
func (s *SQLiteStore) RecordDelivery(ctx context.Context, delivery *drevtypes.WebhookDelivery) error {
	if delivery == nil || delivery.DeliveryID == "" {
		return errors.New("delivery cannot be nil or empty")
	}
	_, err := s.stmtRecordDelivery.ExecContext(ctx,
		delivery.DeliveryID, delivery.EventType, delivery.Repo, delivery.PRNumber, delivery.RunID,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "constraint failed") {
			return ErrDuplicateDelivery
		}
		return fmt.Errorf("recording webhook delivery: %w", err)
	}
	return nil
}

// GetDelivery retrieves the webhook delivery record for a delivery ID.
func (s *SQLiteStore) GetDelivery(ctx context.Context, deliveryID string) (*drevtypes.WebhookDelivery, error) {
	var d drevtypes.WebhookDelivery
	var createdAt sql.NullString
	err := s.stmtGetDelivery.QueryRowContext(ctx, deliveryID).Scan(
		&d.DeliveryID, &d.EventType, &d.Repo, &d.PRNumber, &d.RunID, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting webhook delivery: %w", err)
	}
	d.CreatedAt = scanTime(&createdAt)
	return &d, nil
}

// FailRun marks a run and its jobs as failed with terminal timestamps.
// If jobIDs is empty, it queries the store for any existing jobs belonging to the run.
// It logs any update failures and returns an aggregate error if any updates failed.
func FailRun(ctx context.Context, s Store, runID string, jobIDs []string) error {
	if s == nil {
		return nil
	}
	bgCtx := context.Background()
	var errs []error

	if err := s.UpdateRunStatus(bgCtx, runID, drevtypes.StatusFailed); err != nil {
		log.Printf("[store] failed to update run %s status to failed: %v", runID, err)
		errs = append(errs, fmt.Errorf("updating run %s status: %w", runID, err))
	}

	if len(jobIDs) == 0 {
		if jobs, err := s.GetRunJobs(bgCtx, runID); err == nil {
			for _, j := range jobs {
				jobIDs = append(jobIDs, j.ID)
			}
		} else {
			log.Printf("[store] failed to query run jobs for run %s: %v", runID, err)
			errs = append(errs, fmt.Errorf("querying jobs for run %s: %w", runID, err))
		}
	}

	for _, jid := range jobIDs {
		if err := s.UpdateRunJobStatus(bgCtx, jid, drevtypes.StatusFailed); err != nil {
			log.Printf("[store] failed to update run job %s status to failed: %v", jid, err)
			errs = append(errs, fmt.Errorf("updating job %s status: %w", jid, err))
		}
	}

	return errors.Join(errs...)
}
