package drevtypes

import (
	"testing"
	"time"
)

func TestDeduceInterruptedStage(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name     string
		run      *Run
		jobs     []*RunJob
		expected string
	}{
		{
			name:     "nil run",
			run:      nil,
			jobs:     nil,
			expected: StageUnknown,
		},
		{
			name: "pending run",
			run: &Run{
				ID:     "run-1",
				Status: StatusPending,
			},
			jobs:     nil,
			expected: StageScheduling,
		},
		{
			name: "already terminal run",
			run: &Run{
				ID:        "run-1",
				Status:    StatusSuccess,
				StartedAt: now,
			},
			jobs:     nil,
			expected: StageUnknown,
		},
		{
			name: "running run with zero started at",
			run: &Run{
				ID:     "run-1",
				Status: StatusRunning,
			},
			jobs:     nil,
			expected: StageUnknown,
		},
		{
			name: "running run with zero jobs",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs:     []*RunJob{},
			expected: StagePreparation,
		},
		{
			name: "running run with all pending jobs",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				{ID: "j1", Status: StatusPending},
				{ID: "j2", Status: StatusPending},
			},
			expected: StagePreparation,
		},
		{
			name: "running run with an actively running job",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				{ID: "j1", Status: StatusRunning},
				{ID: "j2", Status: StatusPending},
			},
			expected: StageExecution,
		},
		{
			name: "running run with partial DAG completed and remaining pending",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				{ID: "j1", Status: StatusSuccess},
				{ID: "j2", Status: StatusPending},
			},
			expected: StageExecution,
		},
		{
			name: "running run with all jobs terminal",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				{ID: "j1", Status: StatusSuccess},
				{ID: "j2", Status: StatusSuccess},
			},
			expected: StageFinalization,
		},
		{
			name: "running run with corrupt job status",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				{ID: "j1", Status: RunStatus("invalid_status")},
			},
			expected: StageUnknown,
		},
		{
			name: "running run with nil job in slice",
			run: &Run{
				ID:        "run-1",
				Status:    StatusRunning,
				StartedAt: now,
			},
			jobs: []*RunJob{
				nil,
			},
			expected: StageUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DeduceInterruptedStage(tc.run, tc.jobs)
			if got != tc.expected {
				t.Fatalf("expected stage %q, got %q", tc.expected, got)
			}
		})
	}
}
