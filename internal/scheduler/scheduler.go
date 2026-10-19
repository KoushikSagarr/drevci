package scheduler

import (
	"context"
	"fmt"
	"io"
	"sync"

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
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return err
	}

	jobDoneChans := make(map[string]chan struct{})
	for _, job := range pipeline.Jobs {
		jobDoneChans[job.Name] = make(chan struct{})
	}

	var mu sync.Mutex
	jobStatuses := make(map[string]drevtypes.RunStatus)

	eg, egCtx := errgroup.WithContext(ctx)

	for _, jobName := range sortedJobs {
		jobName := jobName
		job := jobMap[jobName]

		eg.Go(func() error {
			for _, depName := range job.DependsOn {
				select {
				case <-jobDoneChans[depName]:
				case <-egCtx.Done():
					return egCtx.Err()
				}

				mu.Lock()
				status := jobStatuses[depName]
				mu.Unlock()

				if status != drevtypes.StatusSuccess {
					return fmt.Errorf("dependency %q failed with status %s", depName, status)
				}
			}

			rj, ok := runJobMap[jobName]
			if !ok {
				return fmt.Errorf("run job not found for %s", jobName)
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
				s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusFailed)
				return fmt.Errorf("creating workspace for job %s: %w", job.Name, err)
			}

			s.store.UpdateRunJobStatus(egCtx, rj.ID, drevtypes.StatusRunning)

			err = s.runner.RunJob(egCtx, run, &job, jobWorkspace, logWriter)

			mu.Lock()
			if err != nil {
				jobStatuses[jobName] = drevtypes.StatusFailed
				mu.Unlock()
				s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusFailed)
				return err
			}
			jobStatuses[jobName] = drevtypes.StatusSuccess
			mu.Unlock()

			s.store.UpdateRunJobStatus(context.Background(), rj.ID, drevtypes.StatusSuccess)
			close(jobDoneChans[jobName])
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusFailed)
		return err
	}

	s.store.UpdateRunStatus(context.Background(), runID, drevtypes.StatusSuccess)
	return nil
}
