package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/lib/pq"
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

	initQueries := []string{
		`CREATE TABLE IF NOT EXISTS webhook_deliveries (
			delivery_id TEXT PRIMARY KEY,
			event_type  TEXT NOT NULL,
			repo        TEXT NOT NULL DEFAULT '',
			pr_number   INTEGER NOT NULL DEFAULT 0,
			run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_run_id ON webhook_deliveries(run_id)`,
		`CREATE TABLE IF NOT EXISTS pipeline_outbox (
			run_id        UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
			pipeline_spec TEXT NOT NULL,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pipeline_outbox_created_at ON pipeline_outbox(created_at ASC)`,
		`DO $$
		BEGIN
			-- 1. webhook_deliveries
			IF EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = CURRENT_SCHEMA AND table_name = 'webhook_deliveries'
			) THEN
				IF EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = CURRENT_SCHEMA
					  AND table_name = 'webhook_deliveries'
					  AND column_name = 'run_id'
					  AND data_type = 'text'
				) THEN
					ALTER TABLE webhook_deliveries ALTER COLUMN run_id TYPE UUID USING run_id::uuid;
				END IF;

				IF EXISTS (
					SELECT 1 FROM information_schema.tables
					WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs'
				) AND NOT EXISTS (
					SELECT 1 FROM information_schema.table_constraints
					WHERE table_schema = CURRENT_SCHEMA
					  AND table_name = 'webhook_deliveries'
					  AND constraint_type = 'FOREIGN KEY'
				) THEN
					ALTER TABLE webhook_deliveries ADD CONSTRAINT fk_webhook_deliveries_run_id FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;
				END IF;
			END IF;

			-- 2. pipeline_outbox
			IF EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = CURRENT_SCHEMA AND table_name = 'pipeline_outbox'
			) THEN
				IF EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = CURRENT_SCHEMA
					  AND table_name = 'pipeline_outbox'
					  AND column_name = 'run_id'
					  AND data_type = 'text'
				) THEN
					ALTER TABLE pipeline_outbox ALTER COLUMN run_id TYPE UUID USING run_id::uuid;
				END IF;

				IF EXISTS (
					SELECT 1 FROM information_schema.tables
					WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs'
				) AND NOT EXISTS (
					SELECT 1 FROM information_schema.table_constraints
					WHERE table_schema = CURRENT_SCHEMA
					  AND table_name = 'pipeline_outbox'
					  AND constraint_type = 'FOREIGN KEY'
				) THEN
					ALTER TABLE pipeline_outbox ADD CONSTRAINT fk_pipeline_outbox_run_id FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;
				END IF;
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			-- 3. nullable run org_id
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = CURRENT_SCHEMA
				  AND table_name = 'runs'
				  AND column_name = 'org_id'
				  AND is_nullable = 'NO'
			) THEN
				ALTER TABLE runs ALTER COLUMN org_id DROP NOT NULL;
			END IF;
		END $$;`,
	}
	for _, q := range initQueries {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, fmt.Errorf("initializing postgres webhook_deliveries: %w", err)
		}
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

// CreateRunWithLimit atomically checks organization capacity with row-level locking
// and creates a run with its jobs in a single transaction.
func (s *PostgresStore) CreateRunWithLimit(ctx context.Context, run *drevtypes.Run, jobs []*drevtypes.RunJob, workerLimit int) error {
	if run == nil || strings.TrimSpace(run.ID) == "" {
		return errors.New("run or run.ID cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning postgres transaction: %w", err)
	}
	defer tx.Rollback()

	if workerLimit > 0 {
		if strings.TrimSpace(run.OrgID) == "" {
			return errors.New("cannot enforce worker limit without valid org_id")
		}

		// 1. Lock the organization row for update to serialize concurrent reservations for this tenant.
		// Unrelated organizations are not locked and proceed in parallel.
		var lockedOrgID string
		err := tx.QueryRowContext(ctx, `
			SELECT id FROM organizations WHERE id = $1 FOR UPDATE`, run.OrgID).Scan(&lockedOrgID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrOrgNotFound
			}
			return fmt.Errorf("locking organization for capacity check: %w", err)
		}

		// 2. Count active runs (pending or running) for this organization.
		var activeRuns int
		err = tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM runs
			WHERE org_id = $1 AND status IN ('running', 'pending')`, run.OrgID).Scan(&activeRuns)
		if err != nil {
			return fmt.Errorf("counting active runs for organization: %w", err)
		}

		// 3. Enforce limit: fail closed if limit reached.
		if activeRuns >= workerLimit {
			return ErrWorkerLimitExceeded
		}
	}

	// 4. Insert parent run
	_, err = tx.ExecContext(ctx, `
		INSERT INTO runs
		  (id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		run.ID, pgNullStr(run.OrgID), run.PipelineID, string(run.Status),
		run.TriggeredBy, pgNullStr(run.CommitSHA), pgNullStr(run.CommitMsg), pgNullStr(run.Branch),
		pgNullTime(run.StartedAt), pgNullTime(run.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("inserting postgres run: %w", err)
	}

	// 5. Insert run jobs
	for _, job := range jobs {
		if job == nil || strings.TrimSpace(job.ID) == "" {
			return errors.New("job or job.ID cannot be empty")
		}
		status := job.Status
		if status == "" {
			status = drevtypes.StatusPending
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO run_jobs (id, run_id, job_name, status)
			VALUES ($1, $2, $3, $4)`,
			job.ID, run.ID, job.JobName, string(status),
		)
		if err != nil {
			return fmt.Errorf("inserting postgres run job: %w", err)
		}
	}

	return tx.Commit()
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
		  finished_at = CASE WHEN finished_at IS NULL AND $4 IN ('success','failed','cancelled') THEN $3 ELSE finished_at END
		WHERE id = $5 AND (status NOT IN ('success', 'failed', 'cancelled') OR status = $6)`,
		string(status), string(status), now, string(status), id, string(status),
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var currentStatus string
		err := s.db.QueryRowContext(ctx, `SELECT status FROM runs WHERE id = $1`, id).Scan(&currentStatus)
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		if err != nil {
			return fmt.Errorf("checking run status for %s: %w", id, err)
		}
		return fmt.Errorf("%w: cannot transition run %s from %s to %s", ErrRunAlreadyTerminal, id, currentStatus, status)
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

// CreateRunJobs atomically inserts a batch of jobs for a run in a single transaction in PostgreSQL.
// If any job insertion or validation fails, zero newly inserted jobs are committed.
func (s *PostgresStore) CreateRunJobs(ctx context.Context, runID string, jobs []*drevtypes.RunJob) error {
	if err := validateRunJobsBatch(runID, jobs); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning postgres transaction: %w", err)
	}
	defer tx.Rollback()

	// Verify target run exists
	var exists int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM runs WHERE id = $1`, runID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("run %s not found: %w", runID, sql.ErrNoRows)
		}
		return fmt.Errorf("verifying postgres run %s: %w", runID, err)
	}

	for _, job := range jobs {
		status := job.Status
		if status == "" {
			status = drevtypes.StatusPending
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			job.ID, runID, job.JobName, string(status),
			pgNullTime(job.StartedAt), pgNullTime(job.FinishedAt),
		)
		if err != nil {
			return fmt.Errorf("inserting postgres run job %s: %w", job.JobName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing postgres transaction: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateRunJobStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE run_jobs SET
		  status      = $1,
		  started_at  = CASE WHEN $2 = 'running'  AND started_at  IS NULL THEN $3 ELSE started_at  END,
		  finished_at = CASE WHEN finished_at IS NULL AND $4 IN ('success','failed','cancelled','skipped') THEN $3 ELSE finished_at END
		WHERE id = $5 AND (status NOT IN ('success', 'failed', 'cancelled', 'skipped') OR status = $6)`,
		string(status), string(status), now, string(status), id, string(status),
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var currentStatus string
		err := s.db.QueryRowContext(ctx, `SELECT status FROM run_jobs WHERE id = $1`, id).Scan(&currentStatus)
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		if err != nil {
			return fmt.Errorf("checking run job status for %s: %w", id, err)
		}
		return fmt.Errorf("%w: cannot transition job %s from %s to %s", ErrJobAlreadyTerminal, id, currentStatus, status)
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

func (s *PostgresStore) ResetGhostRuns(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM runs
		WHERE status = 'running'
		   OR (status = 'pending' AND id NOT IN (SELECT run_id FROM pipeline_outbox))`)
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
		return nil, fmt.Errorf("beginning postgres reset ghost runs transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `
			UPDATE runs SET status = 'failed', finished_at = $1
			WHERE id = $2`, now, id); err != nil {
			return nil, fmt.Errorf("updating postgres ghost run %s: %w", id, err)
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE run_jobs SET status = 'failed', finished_at = $1
			WHERE run_id = $2 AND status IN ('running', 'pending')`, now, id); err != nil {
			return nil, fmt.Errorf("updating postgres ghost run jobs for %s: %w", id, err)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE run_id = $1`, id); err != nil {
			return nil, fmt.Errorf("releasing postgres webhook delivery for ghost run %s: %w", id, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing postgres reset ghost runs: %w", err)
	}

	return ids, nil
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

func (s *PostgresStore) GetRunStats(ctx context.Context, orgID string, sampleLimit int) (*drevtypes.RunStats, error) {
	if sampleLimit <= 0 {
		sampleLimit = 25
	}
	if sampleLimit > 100 {
		sampleLimit = 100
	}

	var (
		termRows *sql.Rows
		err      error
	)
	if orgID != "" {
		termRows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs
			WHERE org_id = $1 AND status IN ('success', 'failed', 'cancelled')
			ORDER BY created_at DESC
			LIMIT $2`, orgID, sampleLimit)
	} else {
		termRows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs
			WHERE status IN ('success', 'failed', 'cancelled')
			ORDER BY created_at DESC
			LIMIT $1`, sampleLimit)
	}
	if err != nil {
		return nil, fmt.Errorf("querying terminal runs: %w", err)
	}
	defer termRows.Close()

	var (
		succeededCount      int
		failedCount         int
		cancelledCount      int
		durationSampleCount int
		totalDurationSec    float64
	)

	for termRows.Next() {
		r, err := scanPgRun(termRows)
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
		if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() && (r.FinishedAt.Equal(r.StartedAt) || r.FinishedAt.After(r.StartedAt)) {
			durationSampleCount++
			totalDurationSec += r.FinishedAt.Sub(r.StartedAt).Seconds()
		}
	}
	if err := termRows.Err(); err != nil {
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

	// Recent failures
	var failRows *sql.Rows
	if orgID != "" {
		failRows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs
			WHERE org_id = $1 AND status = 'failed'
			ORDER BY created_at DESC
			LIMIT 5`, orgID)
	} else {
		failRows, err = s.db.QueryContext(ctx, `
			SELECT id, COALESCE(org_id,''), pipeline_name, status, triggered_by,
			       commit_sha, commit_msg, branch, started_at, finished_at
			FROM runs
			WHERE status = 'failed'
			ORDER BY created_at DESC
			LIMIT 5`)
	}
	if err != nil {
		return nil, fmt.Errorf("querying recent failures: %w", err)
	}
	defer failRows.Close()

	recentFailures := make([]*drevtypes.Run, 0)
	for failRows.Next() {
		r, err := scanPgRun(failRows)
		if err != nil {
			return nil, fmt.Errorf("scanning recent failure: %w", err)
		}
		recentFailures = append(recentFailures, r)
	}
	if err := failRows.Err(); err != nil {
		return nil, fmt.Errorf("reading recent failures: %w", err)
	}

	// Active runs
	var activeQuery string
	var activeArgs []interface{}
	if orgID != "" {
		activeQuery = `SELECT COUNT(*) FROM runs WHERE org_id = $1 AND status IN ('running', 'pending')`
		activeArgs = []interface{}{orgID}
	} else {
		activeQuery = `SELECT COUNT(*) FROM runs WHERE status IN ('running', 'pending')`
	}
	var activeRunsCount int
	if err := s.db.QueryRowContext(ctx, activeQuery, activeArgs...).Scan(&activeRunsCount); err != nil {
		return nil, fmt.Errorf("counting active runs: %w", err)
	}

	// Total runs
	var totalQuery string
	var totalArgs []interface{}
	if orgID != "" {
		totalQuery = `SELECT COUNT(*) FROM runs WHERE org_id = $1`
		totalArgs = []interface{}{orgID}
	} else {
		totalQuery = `SELECT COUNT(*) FROM runs`
	}
	var totalRunsCount int
	if err := s.db.QueryRowContext(ctx, totalQuery, totalArgs...).Scan(&totalRunsCount); err != nil {
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

// CreateRunWithDelivery atomically creates a webhook delivery record, a run, all associated run jobs,
// and a pipeline outbox specification record in PostgreSQL.
// Either all records commit or none commit. Returns ErrDuplicateDelivery on duplicate delivery ID.
func (s *PostgresStore) CreateRunWithDelivery(
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
		return fmt.Errorf("beginning postgres transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Insert run record first so dependent child foreign keys are satisfied
	_, err = tx.ExecContext(ctx, `
		INSERT INTO runs
		  (id, org_id, pipeline_name, status, triggered_by, commit_sha, commit_msg, branch, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		run.ID, pgNullStr(run.OrgID), run.PipelineID, string(run.Status),
		run.TriggeredBy, pgNullStr(run.CommitSHA), pgNullStr(run.CommitMsg), pgNullStr(run.Branch),
		pgNullTime(run.StartedAt), pgNullTime(run.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("inserting postgres run: %w", err)
	}

	// 2. Insert webhook delivery record (child referencing runs(id))
	createdAt := time.Now().UTC()
	if !delivery.CreatedAt.IsZero() {
		createdAt = delivery.CreatedAt.UTC()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO webhook_deliveries (delivery_id, event_type, repo, pr_number, run_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		delivery.DeliveryID, delivery.EventType, delivery.Repo, delivery.PRNumber, delivery.RunID, createdAt,
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return ErrDuplicateDelivery
		}
		if strings.Contains(err.Error(), "unique constraint") || strings.Contains(err.Error(), "duplicate key") {
			return ErrDuplicateDelivery
		}
		return fmt.Errorf("inserting postgres webhook delivery: %w", err)
	}

	// 3. Insert run jobs
	for _, job := range jobs {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO run_jobs (id, run_id, job_name, status, started_at, finished_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			job.ID, job.RunID, job.JobName, string(job.Status),
			pgNullTime(job.StartedAt), pgNullTime(job.FinishedAt),
		)
		if err != nil {
			return fmt.Errorf("inserting postgres run job %s: %w", job.JobName, err)
		}
	}

	// 4. Insert into pipeline_outbox
	_, err = tx.ExecContext(ctx, `
		INSERT INTO pipeline_outbox (run_id, pipeline_spec, created_at)
		VALUES ($1, $2, $3)`,
		run.ID, pipelineSpec, createdAt,
	)
	if err != nil {
		return fmt.Errorf("inserting postgres pipeline outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing postgres transaction: %w", err)
	}
	return nil
}

// ClaimRunForExecution atomically claims a pending run for execution in PostgreSQL.
// It verifies the run is currently pending with started_at IS NULL, transitions it to running,
// sets started_at, and deletes the pipeline_outbox entry in a single transaction.
// Returns ErrRunAlreadyClaimed if the run was already claimed, running, or finished.
func (s *PostgresStore) ClaimRunForExecution(ctx context.Context, runID string) error {
	if runID == "" {
		return errors.New("runID cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning postgres claim transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'running', started_at = $1
		WHERE id = $2 AND status = 'pending' AND started_at IS NULL`,
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

	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_outbox WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("deleting outbox entry for claimed run %s: %w", runID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing claim transaction: %w", err)
	}
	return nil
}

// GetPendingOutbox retrieves up to limit pending, never-started runs from PostgreSQL outbox,
// ordered deterministically by creation time.
func (s *PostgresStore) GetPendingOutbox(ctx context.Context, limit int) ([]*drevtypes.OutboxItem, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT o.run_id, o.pipeline_spec, o.created_at
		FROM pipeline_outbox o
		JOIN runs r ON r.id = o.run_id
		WHERE r.status = 'pending' AND r.started_at IS NULL
		ORDER BY o.created_at ASC, o.run_id ASC
		LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying postgres pending outbox: %w", err)
	}
	defer rows.Close()

	var items []*drevtypes.OutboxItem
	for rows.Next() {
		var item drevtypes.OutboxItem
		var createdAt time.Time
		if err := rows.Scan(&item.RunID, &item.PipelineSpec, &createdAt); err != nil {
			return nil, fmt.Errorf("scanning outbox item: %w", err)
		}
		item.CreatedAt = createdAt
		items = append(items, &item)
	}
	return items, rows.Err()
}

// FailRunWithDelivery atomically marks a run and its jobs as failed with terminal timestamps,
// deletes any matching pipeline_outbox record, and deletes the matching webhook delivery claim in PostgreSQL.
// Any failure rolls back all changes.
func (s *PostgresStore) FailRunWithDelivery(ctx context.Context, runID string, jobIDs []string, deliveryID string) error {
	if runID == "" {
		return errors.New("runID cannot be empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning fail run with delivery transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	// 1. Mark run failed
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'failed',
		    finished_at = CASE WHEN finished_at IS NULL THEN $1 ELSE finished_at END
		WHERE id = $2`, now, runID,
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
				    finished_at = CASE WHEN finished_at IS NULL THEN $1 ELSE finished_at END
				WHERE id = $2 AND run_id = $3`, now, jid, runID)
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
			    finished_at = CASE WHEN finished_at IS NULL THEN $1 ELSE finished_at END
			WHERE run_id = $2`, now, runID); err != nil {
			return fmt.Errorf("updating run jobs for run %s to failed: %w", runID, err)
		}
	}

	// 3. Delete matching outbox record
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_outbox WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("deleting outbox record for run %s: %w", runID, err)
	}

	// 4. Delete matching webhook delivery claim if deliveryID is provided (scoped strictly to this run)
	if deliveryID != "" {
		res, err := tx.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE delivery_id = $1 AND run_id = $2`, deliveryID, runID)
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

// DeleteDelivery removes a webhook delivery record from PostgreSQL.
func (s *PostgresStore) DeleteDelivery(ctx context.Context, deliveryID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE delivery_id = $1`, deliveryID)
	return err
}

// RecordDelivery persists a webhook delivery record in PostgreSQL. Returns ErrDuplicateDelivery on duplicate delivery ID.
func (s *PostgresStore) RecordDelivery(ctx context.Context, delivery *drevtypes.WebhookDelivery) error {
	if delivery == nil || delivery.DeliveryID == "" {
		return errors.New("delivery cannot be nil or empty")
	}
	query := `
		INSERT INTO webhook_deliveries (delivery_id, event_type, repo, pr_number, run_id)
		VALUES ($1, $2, $3, $4, $5)`
	_, err := s.db.ExecContext(ctx, query,
		delivery.DeliveryID, delivery.EventType, delivery.Repo, delivery.PRNumber, delivery.RunID,
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return ErrDuplicateDelivery
		}
		if strings.Contains(err.Error(), "unique constraint") || strings.Contains(err.Error(), "duplicate key") {
			return ErrDuplicateDelivery
		}
		return fmt.Errorf("recording postgres webhook delivery: %w", err)
	}
	return nil
}

// GetDelivery retrieves the webhook delivery record from PostgreSQL.
func (s *PostgresStore) GetDelivery(ctx context.Context, deliveryID string) (*drevtypes.WebhookDelivery, error) {
	query := `
		SELECT delivery_id, event_type, repo, pr_number, run_id, created_at
		FROM webhook_deliveries
		WHERE delivery_id = $1`
	var d drevtypes.WebhookDelivery
	var createdAt sql.NullTime
	err := s.db.QueryRowContext(ctx, query, deliveryID).Scan(
		&d.DeliveryID, &d.EventType, &d.Repo, &d.PRNumber, &d.RunID, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting postgres webhook delivery: %w", err)
	}
	d.CreatedAt = scanNullTime(&createdAt)
	return &d, nil
}
