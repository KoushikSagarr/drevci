package runner

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/errdefs"
	"github.com/drevci/drev/pkg/drevtypes"
)

const (
	// LabelManagedBy identifies containers created and managed by Drev CI.
	LabelManagedBy = "ci.drev.managed"
	// LabelRunID identifies the pipeline run ID associated with the container.
	LabelRunID = "ci.drev.run_id"
	// LabelJobName identifies the job name within the pipeline.
	LabelJobName = "ci.drev.job_name"
)

// ReconciliationSummary details the outcome of an orphaned container reconciliation pass.
type ReconciliationSummary struct {
	TotalChecked int      // Total Drev CI managed containers inspected
	Stopped      int      // Total containers that were running and stopped
	Removed      int      // Total containers removed
	Errors       []string // Non-fatal errors encountered during reconciliation
}

// ReconcileOrphans reconciles and cleans up leftover/orphaned Drev CI containers on daemon startup.
//
// Recovery Policy:
// 1. Ownership: Only containers with label ci.drev.managed == "true" are considered. Unmanaged containers are never touched.
// 2. Interrupted Runs: Containers corresponding to interrupted runs (runs in 'running' or 'pending' state at crash) are stopped and removed.
// 3. Completed/Failed Runs: Containers corresponding to finalized runs ('success', 'failed', 'cancelled') left behind are stopped (if running) and removed.
// 4. Missing/Untracked Runs: Containers with unknown run IDs not found in the database are stopped (if running) and removed.
// 5. Missing or Already Stopped: Already stopped or missing containers are handled gracefully without error.
// 6. Idempotence: Running ReconcileOrphans repeatedly is strictly idempotent.
func (r *Runner) ReconcileOrphans(ctx context.Context, interruptedRunIDs []string) (*ReconciliationSummary, error) {
	summary := &ReconciliationSummary{}

	containers, err := r.docker.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return summary, fmt.Errorf("listing containers for reconciliation: %w", err)
	}

	interruptedMap := make(map[string]bool, len(interruptedRunIDs))
	for _, id := range interruptedRunIDs {
		interruptedMap[id] = true
	}

	stopTimeout := 5 // 5 seconds graceful stop before SIGKILL

	for _, c := range containers {
		// Rule 1: Strictly ignore any container not positively identified as Drev CI managed.
		if c.Labels == nil || c.Labels[LabelManagedBy] != "true" {
			continue
		}

		summary.TotalChecked++
		runID := c.Labels[LabelRunID]
		jobName := c.Labels[LabelJobName]
		containerID := c.ID

		shouldReconcile := false
		reason := ""

		if runID == "" {
			shouldReconcile = true
			reason = "managed container with empty run ID"
		} else if interruptedMap[runID] {
			shouldReconcile = true
			reason = fmt.Sprintf("interrupted run %s (job %s)", runID, jobName)
		} else if r.store != nil {
			run, err := r.store.GetRun(ctx, runID)
			if err != nil || run == nil {
				shouldReconcile = true
				reason = fmt.Sprintf("run %s not found in store", runID)
			} else {
				shouldReconcile = true
				reason = fmt.Sprintf("run %s has status %s (leftover container)", runID, run.Status)
				if run.Status == drevtypes.StatusRunning || run.Status == drevtypes.StatusPending {
					_ = r.store.UpdateRunStatus(context.Background(), run.ID, drevtypes.StatusFailed)
				}
			}
		} else {
			shouldReconcile = true
			reason = fmt.Sprintf("drev managed container %s", runID)
		}

		if !shouldReconcile {
			continue
		}

		// Stop container if active
		isRunning := strings.HasPrefix(strings.ToLower(c.State), "running") || strings.ToLower(c.Status) == "running"
		if isRunning {
			stopErr := r.docker.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &stopTimeout})
			if stopErr != nil && !isAlreadyStopped(stopErr) && !errdefs.IsNotFound(stopErr) {
				summary.Errors = append(summary.Errors, fmt.Sprintf("stopping container %s (%s): %v", truncateID(containerID), reason, stopErr))
			} else {
				summary.Stopped++
			}
		}

		// Remove container
		rmErr := r.docker.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
		if rmErr != nil && !errdefs.IsNotFound(rmErr) {
			summary.Errors = append(summary.Errors, fmt.Sprintf("removing container %s (%s): %v", truncateID(containerID), reason, rmErr))
		} else {
			summary.Removed++
		}
	}

	return summary, nil
}

func isAlreadyStopped(err error) bool {
	if err == nil {
		return false
	}
	if errdefs.IsConflict(err) || errdefs.IsNotModified(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "is not running") || strings.Contains(msg, "not running") || strings.Contains(msg, "already stopped")
}

func truncateID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// LogReconciliationSummary writes formatted reconciliation messages to the provided logger function (e.g. log.Printf).
// It surfaces top-level and partial errors clearly, distinguishing successes from failures, and never falsely claims full success.
func LogReconciliationSummary(logFn func(format string, v ...any), summary *ReconciliationSummary, topLevelErr error) {
	if logFn == nil {
		return
	}
	if topLevelErr != nil {
		logFn("Warning: orphaned container reconciliation failed: %v", topLevelErr)
	}
	if summary == nil {
		return
	}
	if len(summary.Errors) > 0 {
		for _, errStr := range summary.Errors {
			logFn("Warning: reconciliation error: %s", errStr)
		}
		logFn("Warning: orphaned container reconciliation completed with %d error(s) (%d inspected, %d stopped, %d removed)",
			len(summary.Errors), summary.TotalChecked, summary.Stopped, summary.Removed)
	} else if summary.Removed > 0 || summary.Stopped > 0 {
		logFn("Reconciled %d orphaned Drev CI container(s) (%d stopped, %d removed)", summary.TotalChecked, summary.Stopped, summary.Removed)
	}
}
