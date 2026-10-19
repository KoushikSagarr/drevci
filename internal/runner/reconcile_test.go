package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/errdefs"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
)

// fakeStore implements a minimal in-memory store for runner reconciliation tests.
type fakeStore struct {
	store.Store
	runs map[string]*drevtypes.Run
}

func (f *fakeStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	if r, ok := f.runs[id]; ok {
		return r, nil
	}
	return nil, nil
}

func (f *fakeStore) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	if r, ok := f.runs[id]; ok {
		r.Status = status
	}
	return nil
}

func TestReconcileOrphans_OwnedVsUnrelatedContainers(t *testing.T) {
	fs := &fakeStore{
		runs: map[string]*drevtypes.Run{
			"run-1": {ID: "run-1", Status: drevtypes.StatusFailed},
			"run-2": {ID: "run-2", Status: drevtypes.StatusSuccess},
		},
	}

	stoppedIDs := make(map[string]bool)
	removedIDs := make(map[string]bool)

	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return []types.Container{
				{
					ID:    "drev-container-1",
					State: "running",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "run-1",
						LabelJobName:   "test",
					},
				},
				{
					ID:    "unrelated-postgres-container",
					State: "running",
					Labels: map[string]string{
						"com.docker.compose.project": "my-database",
					},
				},
				{
					ID:    "unrelated-user-app",
					State: "running",
					Names: []string{"/drev-like-name-but-unmanaged"},
				},
			}, nil
		},
		containerStopFn: func(ctx context.Context, containerID string, options container.StopOptions) error {
			stoppedIDs[containerID] = true
			return nil
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			removedIDs[containerID] = true
			return nil
		},
	}

	r := NewWithClient(mock, fs)

	summary, err := r.ReconcileOrphans(context.Background(), []string{"run-1"})
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.TotalChecked != 1 {
		t.Errorf("expected 1 Drev CI container checked, got %d", summary.TotalChecked)
	}
	if summary.Stopped != 1 {
		t.Errorf("expected 1 container stopped, got %d", summary.Stopped)
	}
	if summary.Removed != 1 {
		t.Errorf("expected 1 container removed, got %d", summary.Removed)
	}

	// Verify only drev-container-1 was stopped and removed
	if !stoppedIDs["drev-container-1"] {
		t.Errorf("expected drev-container-1 to be stopped")
	}
	if !removedIDs["drev-container-1"] {
		t.Errorf("expected drev-container-1 to be removed")
	}

	// Verify unrelated containers were NEVER touched
	if stoppedIDs["unrelated-postgres-container"] || removedIDs["unrelated-postgres-container"] {
		t.Fatalf("SAFETY VIOLATION: unrelated container was touched by reconciliation!")
	}
	if stoppedIDs["unrelated-user-app"] || removedIDs["unrelated-user-app"] {
		t.Fatalf("SAFETY VIOLATION: unmanaged container with name pattern was touched!")
	}
}

func TestReconcileOrphans_AlreadyStoppedAndMissing(t *testing.T) {
	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return []types.Container{
				{
					ID:    "stopped-drev-container",
					State: "exited",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "run-ghost",
					},
				},
			}, nil
		},
		containerStopFn: func(ctx context.Context, containerID string, options container.StopOptions) error {
			return errdefs.Conflict(errors.New("container is already stopped"))
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			return nil
		},
	}

	r := NewWithClient(mock, &fakeStore{runs: map[string]*drevtypes.Run{}})

	summary, err := r.ReconcileOrphans(context.Background(), []string{"run-ghost"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(summary.Errors) != 0 {
		t.Errorf("expected 0 errors for already stopped container, got: %v", summary.Errors)
	}
	if summary.Removed != 1 {
		t.Errorf("expected 1 container removed, got %d", summary.Removed)
	}
}

func TestReconcileOrphans_Idempotence(t *testing.T) {
	fs := &fakeStore{runs: map[string]*drevtypes.Run{}}
	containersList := []types.Container{
		{
			ID:    "c-1",
			State: "running",
			Labels: map[string]string{
				LabelManagedBy: "true",
				LabelRunID:     "run-10",
			},
		},
	}

	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return containersList, nil
		},
		containerStopFn: func(ctx context.Context, containerID string, options container.StopOptions) error {
			return nil
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			// Simulate removal for next call
			containersList = nil
			return nil
		},
	}

	r := NewWithClient(mock, fs)

	// First pass: cleans up container
	s1, err := r.ReconcileOrphans(context.Background(), []string{"run-10"})
	if err != nil {
		t.Fatalf("pass 1 failed: %v", err)
	}
	if s1.Removed != 1 {
		t.Errorf("pass 1: expected 1 removed, got %d", s1.Removed)
	}

	// Second pass: nothing left to clean, should be a clean no-op
	s2, err := r.ReconcileOrphans(context.Background(), []string{})
	if err != nil {
		t.Fatalf("pass 2 failed: %v", err)
	}
	if s2.Removed != 0 || s2.Stopped != 0 || len(s2.Errors) != 0 {
		t.Errorf("pass 2 was not idempotent: %+v", s2)
	}
}

func TestReconcileOrphans_DockerAPIError(t *testing.T) {
	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return nil, errors.New("cannot connect to docker daemon")
		},
	}

	r := NewWithClient(mock, &fakeStore{runs: map[string]*drevtypes.Run{}})

	_, err := r.ReconcileOrphans(context.Background(), []string{"run-1"})
	if err == nil {
		t.Fatal("expected error when Docker ContainerList fails, got nil")
	}
}

func TestReconcileOrphans_PartialFailure(t *testing.T) {
	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return []types.Container{
				{
					ID:    "c-broken-remove",
					State: "exited",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "run-1",
					},
				},
				{
					ID:    "c-success-remove",
					State: "exited",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "run-2",
					},
				},
			}, nil
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			if containerID == "c-broken-remove" {
				return errors.New("disk I/O error on removal")
			}
			return nil
		},
	}

	r := NewWithClient(mock, &fakeStore{runs: map[string]*drevtypes.Run{}})

	summary, err := r.ReconcileOrphans(context.Background(), []string{"run-1", "run-2"})
	if err != nil {
		t.Fatalf("unexpected top-level error: %v", err)
	}

	if summary.Removed != 1 {
		t.Errorf("expected 1 successfully removed container, got %d", summary.Removed)
	}
	if len(summary.Errors) != 1 {
		t.Errorf("expected 1 recorded error for broken container, got: %v", summary.Errors)
	}
}

func TestReconcileOrphans_FinalizedSuccessRun(t *testing.T) {
	fs := &fakeStore{
		runs: map[string]*drevtypes.Run{
			"run-success": {ID: "run-success", Status: drevtypes.StatusSuccess},
		},
	}

	stopped := false
	removed := false

	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return []types.Container{
				{
					ID:    "c-leftover-success",
					State: "running",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "run-success",
						LabelJobName:   "build",
					},
				},
			}, nil
		},
		containerStopFn: func(ctx context.Context, containerID string, options container.StopOptions) error {
			stopped = true
			return nil
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			removed = true
			return nil
		},
	}

	r := NewWithClient(mock, fs)

	summary, err := r.ReconcileOrphans(context.Background(), []string{})
	if err != nil {
		t.Fatalf("ReconcileOrphans error: %v", err)
	}
	if summary.Removed != 1 || !removed || !stopped {
		t.Errorf("expected container to be stopped and removed: summary=%+v", summary)
	}

	// Verify database record remains StatusSuccess (not overwritten to failed)
	run, _ := fs.GetRun(context.Background(), "run-success")
	if run.Status != drevtypes.StatusSuccess {
		t.Errorf("expected run status to remain StatusSuccess, got: %s", run.Status)
	}
}

func TestReconcileOrphans_EmptyRunID(t *testing.T) {
	removed := false
	mock := &mockDockerClient{
		containerListFn: func(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
			return []types.Container{
				{
					ID:    "c-empty-run-id",
					State: "exited",
					Labels: map[string]string{
						LabelManagedBy: "true",
						LabelRunID:     "",
					},
				},
			}, nil
		},
		containerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			removed = true
			return nil
		},
	}

	r := NewWithClient(mock, &fakeStore{runs: map[string]*drevtypes.Run{}})
	summary, err := r.ReconcileOrphans(context.Background(), []string{})
	if err != nil {
		t.Fatalf("ReconcileOrphans error: %v", err)
	}
	if summary.Removed != 1 || !removed {
		t.Errorf("expected container with empty run ID to be removed: %+v", summary)
	}
}

func TestLogReconciliationSummary(t *testing.T) {
	t.Run("top-level error only", func(t *testing.T) {
		var lines []string
		logFn := func(format string, v ...any) {
			lines = append(lines, fmt.Sprintf(format, v...))
		}

		LogReconciliationSummary(logFn, nil, errors.New("daemon unavailable"))

		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d: %v", len(lines), lines)
		}
		if !strings.Contains(lines[0], "Warning: orphaned container reconciliation failed: daemon unavailable") {
			t.Errorf("unexpected log message: %s", lines[0])
		}
	})

	t.Run("partial errors with summary warning", func(t *testing.T) {
		var lines []string
		logFn := func(format string, v ...any) {
			lines = append(lines, fmt.Sprintf(format, v...))
		}

		summary := &ReconciliationSummary{
			TotalChecked: 3,
			Stopped:      1,
			Removed:      2,
			Errors: []string{
				"stopping container 123456789012 (run-1): permission denied",
			},
		}

		LogReconciliationSummary(logFn, summary, nil)

		if len(lines) != 2 {
			t.Fatalf("expected 2 log lines, got %d: %v", len(lines), lines)
		}
		if !strings.Contains(lines[0], "Warning: reconciliation error: stopping container 123456789012") {
			t.Errorf("expected first line to surface specific error, got: %s", lines[0])
		}
		if !strings.Contains(lines[1], "Warning: orphaned container reconciliation completed with 1 error(s) (3 inspected, 1 stopped, 2 removed)") {
			t.Errorf("expected summary warning line, got: %s", lines[1])
		}
		for _, line := range lines {
			if strings.HasPrefix(line, "Reconciled") {
				t.Errorf("SAFETY VIOLATION: claimed full success when errors occurred: %s", line)
			}
		}
	})

	t.Run("pure success", func(t *testing.T) {
		var lines []string
		logFn := func(format string, v ...any) {
			lines = append(lines, fmt.Sprintf(format, v...))
		}

		summary := &ReconciliationSummary{
			TotalChecked: 2,
			Stopped:      1,
			Removed:      2,
			Errors:       nil,
		}

		LogReconciliationSummary(logFn, summary, nil)

		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d: %v", len(lines), lines)
		}
		if !strings.Contains(lines[0], "Reconciled 2 orphaned Drev CI container(s) (1 stopped, 2 removed)") {
			t.Errorf("unexpected success log: %s", lines[0])
		}
	})

	t.Run("clean no-op", func(t *testing.T) {
		var lines []string
		logFn := func(format string, v ...any) {
			lines = append(lines, fmt.Sprintf(format, v...))
		}

		summary := &ReconciliationSummary{
			TotalChecked: 0,
			Stopped:      0,
			Removed:      0,
			Errors:       nil,
		}

		LogReconciliationSummary(logFn, summary, nil)

		if len(lines) != 0 {
			t.Errorf("expected 0 log lines on clean no-op, got: %v", lines)
		}
	})
}
