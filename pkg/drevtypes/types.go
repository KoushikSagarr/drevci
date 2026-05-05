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

// Pipeline is the top-level definition parsed from a .drev.yml file.
type Pipeline struct {
	Name     string            `yaml:"name"     json:"name"`
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
	ID         string    `json:"id"`
	PipelineID string    `json:"pipeline_id"`
	Status     RunStatus `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Jobs       []RunJob  `json:"jobs"`
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
