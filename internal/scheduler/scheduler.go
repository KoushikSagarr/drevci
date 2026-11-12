package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// JobRunner defines the interface for executing a job in an isolated workspace.
type JobRunner interface {
	RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error
}

type executionMode int

const (
	modeBootstrap executionMode = iota
	modePreRegisteredLegacy
	modeInMemoryDirect
	modeUnregisteredRepo
)

// Option configures a Scheduler instance.
type Option func(*Scheduler)

// WithHostFallback configures a fallback pipeline specification to be used
// during dynamic repository-backed execution if .drev.yml is genuinely absent.
func WithHostFallback(p *drevtypes.Pipeline) Option {
	return func(s *Scheduler) {
		s.hostFallback = copyPipeline(p)
	}
}

// Scheduler coordinates pipeline and job execution across worker workspaces.
type Scheduler struct {
	runner       JobRunner
	store        store.Store
	hostFallback *drevtypes.Pipeline
}

// New creates a new Scheduler with optional configuration options.
func New(runner JobRunner, store store.Store, opts ...Option) *Scheduler {
	s := &Scheduler{
		runner: runner,
		store:  store,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// copyPipeline creates an independent shallow copy of a pipeline and its job slice/env map.
func copyPipeline(p *drevtypes.Pipeline) *drevtypes.Pipeline {
	if p == nil {
		return nil
	}
	cp := *p
	if p.Jobs != nil {
		cp.Jobs = make([]drevtypes.Job, len(p.Jobs))
		copy(cp.Jobs, p.Jobs)
	}
	if p.Env != nil {
		cp.Env = make(map[string]string, len(p.Env))
		for k, v := range p.Env {
			cp.Env[k] = v
		}
	}
	return &cp
}

// isTerminalRunStatus reports whether a run status is in a terminal state.
func isTerminalRunStatus(status drevtypes.RunStatus) bool {
	return status == drevtypes.StatusSuccess ||
		status == drevtypes.StatusFailed ||
		status == drevtypes.StatusCancelled
}

// finalizePreparationFailure finalizes run status and diagnostic logging when a failure
// occurs before job goroutines are launched. It ensures already-terminal run statuses
// are preserved and never overwritten.
func (s *Scheduler) finalizePreparationFailure(
	ctx context.Context,
	runID string,
	logWriter io.Writer,
	stage string,
	err error,
	existingRun *drevtypes.Run,
) error {
	isCancelled := ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	targetStatus := drevtypes.StatusFailed
	if isCancelled {
		targetStatus = drevtypes.StatusCancelled
	}

	if logWriter != nil {
		if isCancelled {
			fmt.Fprintf(logWriter, "[drev] %s cancelled: %v\n", stage, err)
		} else {
			fmt.Fprintf(logWriter, "[drev] %s failed: %v\n", stage, err)
		}
	}

	prepErr := fmt.Errorf("%s: %w", stage, err)
	if isCancelled && ctx.Err() != nil {
		prepErr = ctx.Err()
	}

	if s.store == nil {
		return prepErr
	}

	// Determine whether the run is already in a terminal state before deciding whether to update.
	run := existingRun
	var lookupErr error
	if run == nil {
		// No pre-fetched run available (e.g. failure occurred during initial lookup);
		// perform fresh lookup using an uncancelled background context.
		run, lookupErr = s.store.GetRun(context.Background(), runID)
	}

	if lookupErr != nil {
		// Run lookup failed; preserve the original preparation error and report the lookup failure.
		return errors.Join(prepErr, fmt.Errorf("checking run status before finalization: %w", lookupErr))
	}

	// If the run is already terminal (e.g. Cancelled, Failed, Success), preserve its existing status.
	if run != nil && isTerminalRunStatus(run.Status) {
		return prepErr
	}

	// Otherwise, update the run status using an uncancelled background context.
	if updateErr := s.store.UpdateRunStatus(context.Background(), runID, targetStatus); updateErr != nil {
		return errors.Join(prepErr, fmt.Errorf("updating run status to %s: %w", targetStatus, updateErr))
	}

	return prepErr
}

// validateJobRecordsCorrespondence ensures persisted RunJob records match pipeline job definitions exactly.
func validateJobRecordsCorrespondence(jobs []drevtypes.Job, runJobs []*drevtypes.RunJob) (map[string]*drevtypes.RunJob, error) {
	if len(runJobs) != len(jobs) {
		return nil, fmt.Errorf("persisted job count mismatch: pipeline defines %d jobs but found %d persisted records", len(jobs), len(runJobs))
	}
	expectedNames := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		expectedNames[j.Name] = true
	}

	runJobMap := make(map[string]*drevtypes.RunJob, len(runJobs))
	for _, rj := range runJobs {
		if rj == nil {
			return nil, errors.New("nil job record found in persisted run jobs")
		}
		if !expectedNames[rj.JobName] {
			return nil, fmt.Errorf("unexpected persisted job %q not defined in pipeline", rj.JobName)
		}
		if _, exists := runJobMap[rj.JobName]; exists {
			return nil, fmt.Errorf("duplicate persisted job %q found in database", rj.JobName)
		}
		runJobMap[rj.JobName] = rj
	}
	for _, j := range jobs {
		if _, ok := runJobMap[j.Name]; !ok {
			return nil, fmt.Errorf("missing persisted job record for %q", j.Name)
		}
	}
	return runJobMap, nil
}

// buildAndValidateDAG merges pipeline environment variables, checks for blank/duplicate job names,
// verifies dependency existence, and computes Kahn's topological sort.
func buildAndValidateDAG(jobs []drevtypes.Job, pipelineEnv map[string]string) ([]string, map[string]drevtypes.Job, error) {
	if len(jobs) == 0 {
		return nil, nil, errors.New("pipeline defines zero jobs")
	}

	inDegree := make(map[string]int)
	graph := make(map[string][]string)
	jobMap := make(map[string]drevtypes.Job, len(jobs))
	seenNames := make(map[string]bool, len(jobs))

	for _, job := range jobs {
		if strings.TrimSpace(job.Name) == "" {
			return nil, nil, errors.New("job name cannot be blank")
		}
		if seenNames[job.Name] {
			return nil, nil, fmt.Errorf("duplicate job name %q in pipeline", job.Name)
		}
		seenNames[job.Name] = true

		inDegree[job.Name] = 0
		graph[job.Name] = []string{}

		mergedEnv := make(map[string]string)
		for k, v := range pipelineEnv {
			mergedEnv[k] = v
		}
		for k, v := range job.Env {
			mergedEnv[k] = v
		}
		job.Env = mergedEnv
		jobMap[job.Name] = job
	}

	for _, job := range jobs {
		for _, dep := range job.DependsOn {
			if !seenNames[dep] {
				return nil, nil, fmt.Errorf("job %q depends on non-existent job %q", job.Name, dep)
			}
			if dep == job.Name {
				return nil, nil, fmt.Errorf("job %q cannot depend on itself", job.Name)
			}
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

	if len(sortedJobs) != len(jobs) {
		return nil, nil, fmt.Errorf("circular dependency detected in scheduler")
	}

	return sortedJobs, jobMap, nil
}

func (s *Scheduler) RunPipeline(ctx context.Context, pipeline *drevtypes.Pipeline, runID string, logWriter io.Writer) error {
	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "loading run from database", err, nil)
	}
	if run == nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "loading run from database", fmt.Errorf("run %s not found in store", runID), nil)
	}
	if isTerminalRunStatus(run.Status) {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "starting run", fmt.Errorf("run %s is already in terminal status %s", runID, run.Status), run)
	}

	if err := s.store.UpdateRunStatus(ctx, runID, drevtypes.StatusRunning); err != nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "starting run", err, run)
	}
	run.Status = drevtypes.StatusRunning

	existingJobs, err := s.store.GetRunJobs(ctx, runID)
	if err != nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "loading run jobs from database", err, run)
	}

	hasRepoURL := strings.TrimSpace(pipeline.Source.URL) != ""
	hasIncomingJobs := len(pipeline.Jobs) > 0
	hasExistingJobs := len(existingJobs) > 0

	var mode executionMode
	switch {
	case !hasIncomingJobs && hasRepoURL && !hasExistingJobs:
		mode = modeBootstrap
	case hasIncomingJobs && hasExistingJobs:
		mode = modePreRegisteredLegacy
	case hasIncomingJobs && !hasRepoURL:
		mode = modeInMemoryDirect
	case !hasIncomingJobs && hasRepoURL && hasExistingJobs:
		return s.finalizePreparationFailure(ctx, runID, logWriter, "pipeline validation",
			fmt.Errorf("inconsistent bootstrap run %s: zero jobs specified but %d job records already exist in database", runID, len(existingJobs)), run)
	case hasIncomingJobs && hasRepoURL && !hasExistingJobs:
		mode = modeUnregisteredRepo
	case !hasIncomingJobs && !hasRepoURL:
		return s.finalizePreparationFailure(ctx, runID, logWriter, "pipeline validation",
			errors.New("invalid pipeline: zero jobs defined and no source URL configured"), run)
	default:
		return s.finalizePreparationFailure(ctx, runID, logWriter, "pipeline validation",
			errors.New("invalid pipeline: inconsistent job and source configuration"), run)
	}

	w, err := workspace.Create()
	if err != nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "workspace creation", err, run)
	}
	defer w.Cleanup()

	needsClone := mode == modeBootstrap || mode == modeUnregisteredRepo || (mode == modePreRegisteredLegacy && hasRepoURL)
	if needsClone {
		if err := w.Clone(ctx, pipeline.Source, logWriter); err != nil {
			return s.finalizePreparationFailure(ctx, runID, logWriter, "repository clone", err, run)
		}
	}

	var activePipeline *drevtypes.Pipeline
	switch mode {
	case modeBootstrap:
		resolved, err := w.ResolveConfig(pipeline.Source, copyPipeline(s.hostFallback), logWriter)
		if err != nil {
			return s.finalizePreparationFailure(ctx, runID, logWriter, "resolving repository configuration", err, run)
		}
		activePipeline = resolved

	case modeUnregisteredRepo:
		resolved, err := w.ResolveConfig(pipeline.Source, copyPipeline(pipeline), logWriter)
		if err != nil {
			return s.finalizePreparationFailure(ctx, runID, logWriter, "resolving repository configuration", err, run)
		}
		activePipeline = resolved

	case modePreRegisteredLegacy:
		activePipeline = pipeline

	case modeInMemoryDirect:
		activePipeline = pipeline
	}

	if activePipeline == nil || len(activePipeline.Jobs) == 0 {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "pipeline validation", errors.New("resolved pipeline contains zero jobs"), run)
	}

	sortedJobs, jobMap, err := buildAndValidateDAG(activePipeline.Jobs, activePipeline.Env)
	if err != nil {
		return s.finalizePreparationFailure(ctx, runID, logWriter, "pipeline DAG validation", err, run)
	}

	var runJobMap map[string]*drevtypes.RunJob
	if mode == modePreRegisteredLegacy || (mode == modeInMemoryDirect && hasExistingJobs) {
		correspMap, err := validateJobRecordsCorrespondence(activePipeline.Jobs, existingJobs)
		if err != nil {
			return s.finalizePreparationFailure(ctx, runID, logWriter, "validating persisted job records", err, run)
		}
		runJobMap = correspMap
	} else {
		newRunJobs := make([]*drevtypes.RunJob, 0, len(activePipeline.Jobs))
		for _, job := range activePipeline.Jobs {
			rj := &drevtypes.RunJob{
				ID:      uuid.New().String(),
				RunID:   runID,
				JobName: job.Name,
				Status:  drevtypes.StatusPending,
			}
			newRunJobs = append(newRunJobs, rj)
		}

		if err := s.store.CreateRunJobs(ctx, runID, newRunJobs); err != nil {
			return s.finalizePreparationFailure(ctx, runID, logWriter, "registering run jobs", err, run)
		}

		runJobMap = make(map[string]*drevtypes.RunJob, len(newRunJobs))
		for _, rj := range newRunJobs {
			runJobMap[rj.JobName] = rj
		}
	}

	// ── Shared Job Execution Machinery ──────────────────────────────────────────
	jobDoneChans := make(map[string]chan struct{})
	for _, job := range activePipeline.Jobs {
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
