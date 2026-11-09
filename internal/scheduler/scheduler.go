package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
	"golang.org/x/sync/errgroup"
)

// JobRunner defines the interface for executing a job in an isolated workspace.
type JobRunner interface {
	RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error
}

type Scheduler struct {
	runner JobRunner
	store  store.Store
}

func New(runner JobRunner, store store.Store) *Scheduler {
	return &Scheduler{
		runner: runner,
		store:  store,
	}
}

func (s *Scheduler) RunPipeline(ctx context.Context, pipeline *drevtypes.Pipeline, runID string, logWriter io.Writer) error {
	if err := s.store.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		return fmt.Errorf("updating run status: %w", err)
	}

	inDegree := make(map[string]int)
	graph := make(map[string][]string)
	jobMap := make(map[string]drevtypes.Job)

	for _, job := range pipeline.Jobs {
		inDegree[job.Name] = 0
		graph[job.Name] = []string{}

		mergedEnv := make(map[string]string)
		for k, v := range pipeline.Env {
			mergedEnv[k] = v
		}
		for k, v := range job.Env {
			mergedEnv[k] = v
		}
		job.Env = mergedEnv
		jobMap[job.Name] = job
	}

	for _, job := range pipeline.Jobs {
		for _, dep := range job.DependsOn {
			graph[dep] = append(graph[dep], job.Name)
			inDegree[job.Name]++
		}
	}

	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var sortedJobs []string
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		sortedJobs = append(sortedJobs, curr)

		for _, neighbor := range graph[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sortedJobs) != len(pipeline.Jobs) {
		err := fmt.Errorf("circular dependency detected in scheduler")
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return err
	}

	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("getting run: %w", err)
	}

	runJobs, err := s.store.GetRunJobs(ctx, runID)
	if err != nil {
		return fmt.Errorf("getting run jobs: %w", err)
	}
	runJobMap := make(map[string]*drevtypes.RunJob)
	for _, rj := range runJobs {
		runJobMap[rj.JobName] = rj
	}

	w, err := workspace.Create()
	if err != nil {
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return err
	}
	defer w.Cleanup()

	if err := w.Clone(ctx, pipeline.Source, logWriter); err != nil {
		if ctx.Err() != nil {
			s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusCancelled)
			return ctx.Err()
		}
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return err
	}

	jobDoneChans := make(map[string]chan struct{})
	for _, job := range pipeline.Jobs {
		jobDoneChans[job.Name] = make(chan struct{})
	}

	var mu sync.Mutex
	jobStatuses := make(map[string]drevtypes.RunStatus)

	var eg errgroup.Group

	for _, jobName := range sortedJobs {
		jobName := jobName
		job := jobMap[jobName]

		eg.Go(func() error {
			defer close(jobDoneChans[jobName])

			rj, ok := runJobMap[jobName]
			if !ok {
				mu.Lock()
				jobStatuses[jobName] = drevtypes.StatusFailed
				mu.Unlock()
				return fmt.Errorf("run job not found for %s", jobName)
			}

			if len(job.DependsOn) > 0 {
				depDone := make(chan string, len(job.DependsOn))
				for _, dep := range job.DependsOn {
					dep := dep
					go func() {
						select {
						case <-jobDoneChans[dep]:
							depDone <- dep
						case <-ctx.Done():
						}
					}()
				}

				completedDeps := 0
				var failedDep string
				var failedDepStatus drevtypes.RunStatus

				for completedDeps < len(job.DependsOn) {
					select {
					case dep := <-depDone:
						completedDeps++
						mu.Lock()
						status := jobStatuses[dep]
						mu.Unlock()

						if status != drevtypes.StatusSuccess && failedDep == "" {
							failedDep = dep
							failedDepStatus = status
						}
					case <-ctx.Done():
						mu.Lock()
						jobStatuses[jobName] = drevtypes.StatusCancelled
						mu.Unlock()
						s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusCancelled)
						return ctx.Err()
					}

					// If any dependency failed or was skipped, this job cannot execute.
					// We can skip immediately without waiting for remaining dependencies.
					if failedDep != "" {
						break
					}
				}

				if failedDep != "" {
					fmt.Fprintf(logWriter, "[drev] job %q skipped because dependency %q was %s\n", job.Name, failedDep, failedDepStatus)
					if err := s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusSkipped); err != nil {
						mu.Lock()
						jobStatuses[jobName] = drevtypes.StatusFailed
						mu.Unlock()
						return fmt.Errorf("updating job status to skipped for %s: %w", job.Name, err)
					}
					mu.Lock()
					jobStatuses[jobName] = drevtypes.StatusSkipped
					mu.Unlock()
					return nil
				}
			}

			// Single-Parent Snapshot Inheritance (Contract Rule 3, 4, 5):
			// - Exactly 1 dependency: copy from that dependency's successfully completed workspace
			// - 0 dependencies (root) or >1 dependencies: copy from central source
			var parentJob string
			if len(job.DependsOn) == 1 {
				parentJob = job.DependsOn[0]
			}

			jobWorkspace, err := w.CreateJobWorkspace(job.Name, parentJob)
			if err != nil {
				mu.Lock()
				jobStatuses[jobName] = drevtypes.StatusFailed
				mu.Unlock()
				s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusFailed)
				return fmt.Errorf("creating workspace for job %s: %w", job.Name, err)
			}

			// Determine execution context, enforcing per-job timeout if configured
			jobCtx := ctx
			if job.Timeout != "" {
				d, parseErr := time.ParseDuration(job.Timeout)
				if parseErr != nil || d <= 0 {
					mu.Lock()
					jobStatuses[jobName] = drevtypes.StatusFailed
					fmt.Fprintf(logWriter, "[drev] invalid timeout %q for job %q\n", job.Timeout, job.Name)
					mu.Unlock()
					s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusFailed)
					return fmt.Errorf("invalid timeout %q for job %q", job.Timeout, job.Name)
				}
				var jobCancel context.CancelFunc
				jobCtx, jobCancel = context.WithTimeout(ctx, d)
				defer jobCancel()
			}

			if err := s.store.UpdateRunJobStatus(ctx, rj.ID, drevtypes.StatusRunning); err != nil {
				mu.Lock()
				jobStatuses[jobName] = drevtypes.StatusFailed
				mu.Unlock()
				return fmt.Errorf("updating job status to running for %s: %w", job.Name, err)
			}

			err = s.runner.RunJob(jobCtx, run, &job, jobWorkspace, logWriter)

			mu.Lock()
			if err != nil {
				targetStatus := drevtypes.StatusFailed
				if ctx.Err() == context.Canceled && (errors.Is(err, context.Canceled) || jobCtx.Err() == context.Canceled) {
					targetStatus = drevtypes.StatusCancelled
				}
				jobStatuses[jobName] = targetStatus

				// Report a job timeout only when this job had an explicit timeout configured,
				// its own deadline was exceeded, and the parent context was still active
				// (i.e. ctx.Err() == nil) when the timeout was observed.
				isJobTimeout := job.Timeout != "" &&
					(jobCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded)) &&
					ctx.Err() == nil

				if isJobTimeout {
					fmt.Fprintf(logWriter, "[drev] job %q timed out after %s\n", job.Name, job.Timeout)
					err = fmt.Errorf("job %q timed out after %s: %w", job.Name, job.Timeout, err)
				}
				mu.Unlock()
				s.store.UpdateRunJobStatus(context.Background(), rj.ID, targetStatus)
				return err
			}
			mu.Unlock()

			if err := s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusSuccess); err != nil {
				mu.Lock()
				jobStatuses[jobName] = drevtypes.StatusFailed
				mu.Unlock()
				return fmt.Errorf("updating job status to success for %s: %w", job.Name, err)
			}

			mu.Lock()
			jobStatuses[jobName] = drevtypes.StatusSuccess
			mu.Unlock()
			return nil
		})
	}

	waitErr := eg.Wait()

	mu.Lock()
	var hasFailure, hasSkip, hasCancelled bool
	for _, status := range jobStatuses {
		switch status {
		case drevtypes.StatusFailed:
			hasFailure = true
		case drevtypes.StatusSkipped:
			hasSkip = true
		case drevtypes.StatusCancelled:
			hasCancelled = true
		}
	}
	mu.Unlock()

	if ctx.Err() == context.Canceled || hasCancelled {
		if hasFailure {
			s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
			if waitErr != nil {
				return waitErr
			}
			return fmt.Errorf("pipeline run failed with errors before cancellation")
		}
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusCancelled)
		if waitErr != nil {
			return waitErr
		}
		return ctx.Err()
	}

	if waitErr != nil {
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return waitErr
	}

	if hasFailure || hasSkip {
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return fmt.Errorf("pipeline run completed with failed or skipped jobs")
	}

	s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusSuccess)
	return nil
}
