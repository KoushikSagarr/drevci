package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	_ "github.com/lib/pq"
)

// PostgresStore implements Store backed by a PostgreSQL database (Supabase).
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres connects to a PostgreSQL database, verifies connectivity,
// and returns a PostgresStore ready to use.
func OpenPostgres(connString string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("opening postgres: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// Close closes the underlying database connection pool.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func pgNullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func pgNullStr(str string) interface{} {
	if str == "" {
		return nil
	}
	return str
}

func scanNullTime(nt *sql.NullTime) time.Time {
	if !nt.Valid {
		return time.Time{}
	}
	return nt.Time
}

func scanNullString(ns *sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}

func scanPgRun(row interface {
	Scan(...interface{}) error
}) (*drevtypes.Run, error) {
	var r drevtypes.Run
	var status, triggeredBy string
	var commitSHA, commitMsg, branch sql.NullString
	var startedAt, finishedAt sql.NullTime

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
	r.CommitSHA = scanNullString(&commitSHA)
	r.CommitMsg = scanNullString(&commitMsg)
	r.Branch = scanNullString(&branch)
	r.StartedAt = scanNullTime(&startedAt)
	r.FinishedAt = scanNullTime(&finishedAt)
	return &r, nil
}

// ─── Core pipeline methods ────────────────────────────────────────────────────

func (s *PostgresStore) CreateRun(ctx context.Context, run *drevtypes.Run) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO runs
		  (id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		run.ID, pgNullStr(run.OrgID), run.PipelineID, string(run.Status),
		run.TriggeredBy, pgNullStr(run.CommitSHA), pgNullStr(run.CommitMsg), pgNullStr(run.Branch),
		pgNullTime(run.StartedAt), pgNullTime(run.FinishedAt),
	)
	return err
}

func (s *PostgresStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
		       commit_sha, commit_msg, branch, started_at, finished_at
		FROM runs WHERE id = $1`, id)

	r, err := scanPgRun(row)
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

func (s *PostgresStore) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE runs SET
		  status      = $1,
		  started_at  = CASE WHEN $2 = 'running'  AND started_at  IS NULL THEN $3 ELSE started_at  END,
		  finished_at = CASE WHEN $4 IN ('success','failed','cancelled') THEN $3 ELSE finished_at END
		WHERE id = $5`,
		string(status), string(status), now, string(status), id,
	)
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

func (s *PostgresStore) ListRuns(ctx context.Context, limit int) ([]*drevtypes.Run, error) {
	return s.listRuns(ctx, "", limit)
}

func (s *PostgresStore) ListRunsByOrg(ctx context.Context, orgID string, limit int) ([]*drevtypes.Run, error) {
	return s.listRuns(ctx, orgID, limit)
}

func (s *PostgresStore) listRuns(ctx context.Context, orgID string, limit int) ([]*drevtypes.Run, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if orgID != "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs WHERE org_id = $1
			ORDER BY created_at DESC LIMIT $2`, orgID, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs ORDER BY created_at DESC LIMIT $1`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*drevtypes.Run
	for rows.Next() {
		r, err := scanPgRun(rows)
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

func (s *PostgresStore) CreateRunJob(ctx context.Context, job *drevtypes.RunJob) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		job.ID, job.RunID, job.JobName, string(job.Status),
		pgNullTime(job.StartedAt), pgNullTime(job.FinishedAt),
	)
	return err
}

func (s *PostgresStore) UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE run_jobs SET
		  status      = $1,
		  started_at  = CASE WHEN $2 = 'running'  AND started_at  IS NULL THEN $3 ELSE started_at  END,
		  finished_at = CASE WHEN $4 IN ('success','failed','cancelled') THEN $3 ELSE finished_at END
		WHERE id = $5`,
		string(status), string(status), now, string(status), id,
	)
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

func (s *PostgresStore) GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, job_name, status, started_at, finished_at
		FROM run_jobs WHERE run_id = $1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*drevtypes.RunJob
	for rows.Next() {
		var j drevtypes.RunJob
		var status string
		var startedAt, finishedAt sql.NullTime

		if err := rows.Scan(&j.ID, &j.RunID, &j.JobName, &status, &startedAt, &finishedAt); err != nil {
			return nil, err
		}
		j.Status = drevtypes.RunStatus(status)
		j.StartedAt = scanNullTime(&startedAt)
		j.FinishedAt = scanNullTime(&finishedAt)
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

func (s *PostgresStore) ResetGhostRuns(ctx context.Context) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs SET status = 'failed', finished_at = $1
		WHERE status IN ('running', 'pending')`, now)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE run_jobs SET status = 'failed', finished_at = $1
		WHERE status IN ('running', 'pending')`, now)
	return err
}

// ─── Org management ───────────────────────────────────────────────────────────

func (s *PostgresStore) CreateOrg(ctx context.Context, org *drevtypes.Org) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO organizations (name, slug, plan, worker_limit, queue_limit, log_retention_days)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		org.Name, org.Slug, org.Plan, org.WorkerLimit, org.QueueLimit, org.LogRetentionDays,
	).Scan(&org.ID, &org.CreatedAt)
}

func (s *PostgresStore) GetOrgBySlug(ctx context.Context, slug string) (*drevtypes.Org, error) {
	return s.scanOrg(s.db.QueryRowContext(ctx, `
		SELECT id, name, slug, plan, worker_limit, queue_limit, log_retention_days, created_at
		FROM organizations WHERE slug = $1`, slug))
}

func (s *PostgresStore) GetOrgByID(ctx context.Context, id string) (*drevtypes.Org, error) {
	return s.scanOrg(s.db.QueryRowContext(ctx, `
		SELECT id, name, slug, plan, worker_limit, queue_limit, log_retention_days, created_at
		FROM organizations WHERE id = $1`, id))
}

func (s *PostgresStore) scanOrg(row *sql.Row) (*drevtypes.Org, error) {
	var org drevtypes.Org
	err := row.Scan(
		&org.ID, &org.Name, &org.Slug, &org.Plan,
		&org.WorkerLimit, &org.QueueLimit, &org.LogRetentionDays, &org.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// ─── Token management ─────────────────────────────────────────────────────────

func (s *PostgresStore) CreateToken(ctx context.Context, token *drevtypes.APIToken) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO api_tokens (org_id, name, token_hash, created_by, expires_at)
		VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
		token.OrgID, token.Name, token.TokenHash, token.CreatedBy, token.ExpiresAt,
	).Scan(&token.ID, &token.CreatedAt)
}

func (s *PostgresStore) ValidateToken(ctx context.Context, tokenHash string) (*drevtypes.APIToken, error) {
	var t drevtypes.APIToken
	var lastUsed sql.NullTime
	var expiresAt sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT id, org_id, name, token_hash, last_used_at, created_by, created_at, expires_at
		FROM api_tokens WHERE token_hash = $1`, tokenHash,
	).Scan(&t.ID, &t.OrgID, &t.Name, &t.TokenHash, &lastUsed, &t.CreatedBy, &t.CreatedAt, &expiresAt)
	if err != nil {
		return nil, err
	}

	if lastUsed.Valid {
		t.LastUsedAt = &lastUsed.Time
	}
	if expiresAt.Valid {
		t.ExpiresAt = &expiresAt.Time
		if time.Now().After(*t.ExpiresAt) {
			return nil, fmt.Errorf("token expired")
		}
	}

	// Update last_used_at asynchronously — don't block the request
	go func() {
		s.db.Exec(`UPDATE api_tokens SET last_used_at = NOW() WHERE id = $1`, t.ID) //nolint:errcheck
	}()

	return &t, nil
}

func (s *PostgresStore) GetOrgFromToken(ctx context.Context, tokenHash string) (*drevtypes.Org, error) {
	token, err := s.ValidateToken(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	return s.GetOrgByID(ctx, token.OrgID)
}

// ─── Usage tracking ───────────────────────────────────────────────────────────

func (s *PostgresStore) RecordUsage(ctx context.Context, orgID, eventType, runID string) error {
	var runIDParam interface{}
	if runID != "" {
		runIDParam = runID
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_events (org_id, event_type, run_id, quantity)
		VALUES ($1, $2, $3, 1)`,
		orgID, eventType, runIDParam,
	)
	return err
}

func (s *PostgresStore) GetMonthlyUsage(ctx context.Context, orgID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM usage_events
		WHERE org_id = $1
		  AND event_type = 'pipeline_run'
		  AND recorded_at >= date_trunc('month', NOW())`,
		orgID,
	).Scan(&count)
	return count, err
}
