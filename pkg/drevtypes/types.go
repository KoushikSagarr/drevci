package drevtypes

import "time"

// RunStatus represents the execution state of a pipeline run or job.
type RunStatus string

const (
	StatusPending   RunStatus = "pending"
	StatusRunning   RunStatus = "running"
	StatusSuccess   RunStatus = "success"
	StatusFailed    RunStatus = "failed"
	StatusCancelled RunStatus = "cancelled"
)

// Source defines where the pipeline code comes from.
type Source struct {
	Type string `yaml:"type" json:"type"`
	URL  string `yaml:"url"  json:"url"`
	Ref  string `yaml:"ref"  json:"ref"`
}

// Pipeline is the top-level definition parsed from a .drev.yml file.
type Pipeline struct {
	Name     string            `yaml:"name"     json:"name"`
	Source   Source            `yaml:"source"   json:"source"`
	Triggers []string          `yaml:"triggers" json:"triggers"`
	Env      map[string]string `yaml:"env"      json:"env"`
	Jobs     []Job             `yaml:"jobs"     json:"jobs"`
}

// Job defines a single unit of work within a pipeline.
type Job struct {
	Name      string            `yaml:"name"       json:"name"`
	Image     string            `yaml:"image"      json:"image"`
	Steps     []Step            `yaml:"steps"      json:"steps"`
	Env       map[string]string `yaml:"env"        json:"env"`
	DependsOn []string          `yaml:"depends_on" json:"depends_on"`
}

// Step is an individual command or action executed inside a job container.
type Step struct {
	Name string            `yaml:"name" json:"name"`
	Run  string            `yaml:"run"  json:"run"`
	Env  map[string]string `yaml:"env"  json:"env"`
}

// Run represents a single execution instance of a pipeline.
type Run struct {
	ID           string    `json:"id"`
	OrgID        string    `json:"org_id,omitempty"`
	PipelineID   string    `json:"pipeline_id"`
	PipelineName string    `json:"pipeline_name,omitempty"`
	Status       RunStatus `json:"status"`
	TriggeredBy  string    `json:"triggered_by,omitempty"`
	CommitSHA    string    `json:"commit_sha,omitempty"`
	CommitMsg    string    `json:"commit_msg,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Jobs         []RunJob  `json:"jobs"`
}

// RunJob tracks the execution state of an individual job within a run.
type RunJob struct {
	ID         string    `json:"id"`
	RunID      string    `json:"run_id"`
	JobName    string    `json:"job_name"`
	Status     RunStatus `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// ─── Multi-tenant SaaS types ─────────────────────────────────────────────────

// Org represents a tenant (customer organization).
type Org struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Slug             string    `json:"slug"`
	Plan             string    `json:"plan"`
	WorkerLimit      int       `json:"worker_limit"`
	QueueLimit       int       `json:"queue_limit"`
	LogRetentionDays int       `json:"log_retention_days"`
	CreatedAt        time.Time `json:"created_at"`
}

// OrgMember links a Supabase auth user to an org with a role.
type OrgMember struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"` // owner | admin | member | viewer
	CreatedAt time.Time `json:"created_at"`
}

// APIToken is an org-scoped API token (hash stored, never plaintext).
type APIToken struct {
	ID          string     `json:"id"`
	OrgID       string     `json:"org_id"`
	Name        string     `json:"name"`
	TokenHash   string     `json:"-"` // never serialise the hash
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	CreatedBy   string     `json:"created_by"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// UsageEvent records a billable or metered action for an org.
type UsageEvent struct {
	ID         string    `json:"id"`
	OrgID      string    `json:"org_id"`
	EventType  string    `json:"event_type"` // pipeline_run | worker_minute
	RunID      string    `json:"run_id,omitempty"`
	Quantity   float64   `json:"quantity"`
	RecordedAt time.Time `json:"recorded_at"`
}
