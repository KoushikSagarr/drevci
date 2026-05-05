package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	id TEXT PRIMARY KEY,
	pipeline_name TEXT NOT NULL,
	status TEXT NOT NULL,
	started_at DATETIME,
	finished_at DATETIME,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS run_jobs (
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL REFERENCES runs(id),
	job_name TEXT NOT NULL,
	status TEXT NOT NULL,
	started_at DATETIME,
	finished_at DATETIME
);`

// Store defines the persistence interface for pipeline runs.
type Store interface {
	CreateRun(ctx context.Context, run *drevtypes.Run) error
	GetRun(ctx context.Context, id string) (*drevtypes.Run, error)
	UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	ListRuns(ctx context.Context, limit int) ([]*drevtypes.Run, error)
	CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error
	UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error
	GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error)
}

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

// Open creates or opens a SQLite database at dbPath, initializes
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
		`INSERT INTO runs (id, pipeline_name, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtGetRun, err = s.db.Prepare(
		`SELECT id, pipeline_name, status, started_at, finished_at FROM runs WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtUpdateRunStatus, err = s.db.Prepare(
		`UPDATE runs SET status = ?, 
		 started_at = CASE WHEN ? = 'running' THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE started_at END,
		 finished_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE finished_at END
		 WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtListRuns, err = s.db.Prepare(
		`SELECT id, pipeline_name, status, started_at, finished_at FROM runs ORDER BY created_at DESC LIMIT ?`)
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
		 started_at = CASE WHEN ? = 'running' THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE started_at END,
		 finished_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE finished_at END
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

func (s *SQLiteStore) CreateRun(ctx context.Context, run *drevtypes.Run) error {
	_, err := s.stmtCreateRun.ExecContext(ctx,
		run.ID, run.PipelineID, string(run.Status),
		nullTimeParam(run.StartedAt), nullTimeParam(run.FinishedAt),
	)
	return err
}

func (s *SQLiteStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	var r drevtypes.Run
	var status string
	var startedAt, finishedAt sql.NullString

	err := s.stmtGetRun.QueryRowContext(ctx, id).Scan(
		&r.ID, &r.PipelineID, &status, &startedAt, &finishedAt,
	)
	if err != nil {
		return nil, err
	}
	r.Status = drevtypes.RunStatus(status)
	r.StartedAt = scanTime(&startedAt)
	r.FinishedAt = scanTime(&finishedAt)

	jobs, err := s.GetRunJobs(ctx, id)
	if err != nil {
		return nil, err
	}
	r.Jobs = make([]drevtypes.RunJob, len(jobs))
	for i, j := range jobs {
		r.Jobs[i] = *j
	}

	return &r, nil
}

func (s *SQLiteStore) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	res, err := s.stmtUpdateRunStatus.ExecContext(ctx, string(status), string(status), string(status), id)
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
		var r drevtypes.Run
		var status string
		var startedAt, finishedAt sql.NullString

		if err := rows.Scan(&r.ID, &r.PipelineID, &status, &startedAt, &finishedAt); err != nil {
			return nil, err
		}
		r.Status = drevtypes.RunStatus(status)
		r.StartedAt = scanTime(&startedAt)
		r.FinishedAt = scanTime(&finishedAt)

		jobs, err := s.GetRunJobs(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		r.Jobs = make([]drevtypes.RunJob, len(jobs))
		for i, j := range jobs {
			r.Jobs[i] = *j
		}

		runs = append(runs, &r)
	}

	return runs, rows.Err()
}

func (s *SQLiteStore) CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error {
	_, err := s.stmtCreateRunJob.ExecContext(ctx,
		job.ID, job.RunID, job.JobName, string(job.Status),
		nullTimeParam(job.StartedAt), nullTimeParam(job.FinishedAt),
	)
	return err
}

func (s *SQLiteStore) UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	res, err := s.stmtUpdateRunJobStatus.ExecContext(ctx, string(status), string(status), string(status), id)
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
