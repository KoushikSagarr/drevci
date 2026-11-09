package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

type mockRunner struct {
	mu      sync.Mutex
	runFunc func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error
}

func (m *mockRunner) RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
	m.mu.Lock()
	rf := m.runFunc
	m.mu.Unlock()
	if rf != nil {
		return rf(ctx, run, job, w, logWriter)
	}
	return nil
}

func setupTestPipeline(t *testing.T, pipeline *drevtypes.Pipeline) (store.Store, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	runID := "run-" + fmt.Sprintf("%d", time.Now().UnixNano())
	run := &drevtypes.Run{
		ID:           runID,
		PipelineID:   "pipeline-1",
		PipelineName: pipeline.Name,
		Status:       drevtypes.StatusPending,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("creating test run: %v", err)
	}

	for _, job := range pipeline.Jobs {
		rj := &drevtypes.RunJob{
			ID:        fmt.Sprintf("rj-%s-%d", job.Name, time.Now().UnixNano()),
			RunID:     runID,
			JobName:   job.Name,
			Status:    drevtypes.StatusPending,
			StartedAt: time.Now(),
		}
		if err := s.CreateRunJob(context.Background(), rj); err != nil {
			t.Fatalf("creating test run job: %v", err)
		}
	}

	return s, runID
}

// TestRegression_ParallelRootJobsCannotOverwriteEachOther verifies that two concurrently
// executing root jobs write to independent workspace directories and cannot overwrite
// each other's files.
func TestRegression_ParallelRootJobsCannotOverwriteEachOther(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "parallel-roots",
		Jobs: []drevtypes.Job{
			{Name: "jobA", Image: "alpine"},
			{Name: "jobB", Image: "alpine"},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	// Coordination barriers to guarantee concurrent overlap
	var startWg sync.WaitGroup
	startWg.Add(2)

	jobDirs := make(map[string]string)
	var dirMu sync.Mutex

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			dirMu.Lock()
			jobDirs[job.Name] = w.Dir
			dirMu.Unlock()

			// Write a file with a shared name in this job's workspace
			sharedFile := filepath.Join(w.Dir, "output.txt")
			if err := os.WriteFile(sharedFile, []byte(job.Name), 0644); err != nil {
				return err
			}

			// Synchronize so both jobs are simultaneously active
			startWg.Done()
			startWg.Wait()

			// Sleep briefly to allow any concurrent write race to occur if dirs were shared
			time.Sleep(50 * time.Millisecond)

			// Read back the file from this job's workspace; it MUST still match this job's name
			content, err := os.ReadFile(sharedFile)
			if err != nil {
				return fmt.Errorf("reading sharedFile in %s: %w", job.Name, err)
			}
			if string(content) != job.Name {
				return fmt.Errorf("collision detected in %s: expected %q, got %q", job.Name, job.Name, string(content))
			}

			// Write unique file
			return os.WriteFile(filepath.Join(w.Dir, "unique_"+job.Name+".txt"), []byte("ok"), 0644)
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	dirMu.Lock()
	defer dirMu.Unlock()
	if jobDirs["jobA"] == jobDirs["jobB"] {
		t.Fatalf("expected distinct workspace directories for jobA and jobB, but got identical: %s", jobDirs["jobA"])
	}
}

// TestRegression_TwoConcurrentChildrenOfOneParentReceiveIndependentSnapshots proves
// that two parallel child jobs depending on the same parent both inherit the parent's
// files without colliding when modifying them.
func TestRegression_TwoConcurrentChildrenOfOneParentReceiveIndependentSnapshots(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "fanout",
		Jobs: []drevtypes.Job{
			{Name: "parent", Image: "alpine"},
			{Name: "child1", Image: "alpine", DependsOn: []string{"parent"}},
			{Name: "child2", Image: "alpine", DependsOn: []string{"parent"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var childWg sync.WaitGroup
	childWg.Add(2)

	childSeenValues := make(map[string]string)
	var valMu sync.Mutex

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			sharedPath := filepath.Join(w.Dir, "shared.txt")

			if job.Name == "parent" {
				return os.WriteFile(sharedPath, []byte("parent_initial"), 0644)
			}

			// Both child1 and child2 run concurrently
			initial, err := os.ReadFile(sharedPath)
			if err != nil {
				return fmt.Errorf("%s failed to read parent file: %w", job.Name, err)
			}
			if string(initial) != "parent_initial" {
				return fmt.Errorf("%s saw incorrect parent state: %q", job.Name, string(initial))
			}

			// Mutate file to job-specific value
			if err := os.WriteFile(sharedPath, []byte(job.Name+"_mutated"), 0644); err != nil {
				return err
			}

			childWg.Done()
			childWg.Wait()

			time.Sleep(50 * time.Millisecond)

			finalVal, err := os.ReadFile(sharedPath)
			if err != nil {
				return err
			}

			valMu.Lock()
			childSeenValues[job.Name] = string(finalVal)
			valMu.Unlock()

			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	valMu.Lock()
	defer valMu.Unlock()
	if childSeenValues["child1"] != "child1_mutated" {
		t.Errorf("child1 corrupted: got %q", childSeenValues["child1"])
	}
	if childSeenValues["child2"] != "child2_mutated" {
		t.Errorf("child2 corrupted: got %q", childSeenValues["child2"])
	}
}

// TestSingleParentChildSeesFilesProducedByParent verifies Contract Rule 4:
// jobs with exactly one dependency inherit that parent's workspace.
func TestSingleParentChildSeesFilesProducedByParent(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "linear",
		Jobs: []drevtypes.Job{
			{Name: "build", Image: "alpine"},
			{Name: "deploy", Image: "alpine", DependsOn: []string{"build"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var deploySawArtifact bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "build" {
				return os.WriteFile(filepath.Join(w.Dir, "artifact.bin"), []byte("bin-v1"), 0755)
			}
			if job.Name == "deploy" {
				content, err := os.ReadFile(filepath.Join(w.Dir, "artifact.bin"))
				if err == nil && string(content) == "bin-v1" {
					deploySawArtifact = true
				}
				return err
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if !deploySawArtifact {
		t.Errorf("deploy job did not see artifact created by parent build job")
	}
}

// TestMultiParentChildStartsFromCleanSource verifies Contract Rule 5:
// jobs with multiple dependencies start from the clean central source checkout.
func TestMultiParentChildStartsFromCleanSource(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "diamond",
		Jobs: []drevtypes.Job{
			{Name: "jobA", Image: "alpine"},
			{Name: "jobB", Image: "alpine"},
			{Name: "join", Image: "alpine", DependsOn: []string{"jobA", "jobB"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var joinSawA, joinSawB bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "jobA" {
				return os.WriteFile(filepath.Join(w.Dir, "a_only.txt"), []byte("a"), 0644)
			}
			if job.Name == "jobB" {
				return os.WriteFile(filepath.Join(w.Dir, "b_only.txt"), []byte("b"), 0644)
			}
			if job.Name == "join" {
				if _, err := os.Stat(filepath.Join(w.Dir, "a_only.txt")); err == nil {
					joinSawA = true
				}
				if _, err := os.Stat(filepath.Join(w.Dir, "b_only.txt")); err == nil {
					joinSawB = true
				}
				return nil
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if joinSawA || joinSawB {
		t.Errorf("multi-parent child should start from clean source, but saw parent files: sawA=%v, sawB=%v", joinSawA, joinSawB)
	}
}

// TestFailedDependencyCannotProduceChildSnapshot verifies Contract Rule 6:
// a child must not start from a failed or incomplete parent workspace.
func TestFailedDependencyCannotProduceChildSnapshot(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "fail-propagate",
		Jobs: []drevtypes.Job{
			{Name: "parent", Image: "alpine"},
			{Name: "child", Image: "alpine", DependsOn: []string{"parent"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var childExecuted bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "parent" {
				return errors.New("parent task failed")
			}
			if job.Name == "child" {
				childExecuted = true
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail, got nil")
	}

	if childExecuted {
		t.Errorf("child job should never execute after parent failed")
	}
}

// TestCleanupDoesNotOccurBeforeDependentJobsFinish verifies Contract Rule 8:
// workspace cleanup is deferred until all jobs have completed.
func TestCleanupDoesNotOccurBeforeDependentJobsFinish(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "cleanup-lifecycle",
		Jobs: []drevtypes.Job{
			{Name: "first", Image: "alpine"},
			{Name: "second", Image: "alpine", DependsOn: []string{"first"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var firstDir string
	var secondSawFirstDirValid bool

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "first" {
				firstDir = w.Dir
				return os.WriteFile(filepath.Join(w.Dir, "marker.txt"), []byte("first-done"), 0644)
			}
			if job.Name == "second" {
				// Verify parent's directory has not been cleaned up prematurely
				if fi, err := os.Stat(firstDir); err == nil && fi.IsDir() {
					secondSawFirstDirValid = true
				}
				return nil
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if !secondSawFirstDirValid {
		t.Errorf("parent directory was prematurely cleaned up before second job finished")
	}

	// Now that RunPipeline has returned, the root directory must be cleaned up
	if _, err := os.Stat(firstDir); !os.IsNotExist(err) {
		t.Errorf("expected workspace to be cleaned up after RunPipeline finished, but %s still exists", firstDir)
	}
}

// TestOldSharedWorkspaceDemonstratesCollision directly demonstrates the defect
// that occurred under the old shared workspace model.
func TestOldSharedWorkspaceDemonstratesCollision(t *testing.T) {
	// Simulate old shared workspace: both workers write to the same directory
	sharedDir := t.TempDir()

	var wg sync.WaitGroup
	wg.Add(2)

	results := make([]string, 2)

	worker := func(idx int, content string) {
		defer wg.Done()
		sharedFile := filepath.Join(sharedDir, "config.json")
		_ = os.WriteFile(sharedFile, []byte(content), 0644)
		time.Sleep(30 * time.Millisecond)
		readBack, _ := os.ReadFile(sharedFile)
		results[idx] = string(readBack)
	}

	go worker(0, "payload-from-worker-0")
	go worker(1, "payload-from-worker-1")

	wg.Wait()

	// In a shared directory without isolation, at least one worker will see the other worker's content
	collision := results[0] != "payload-from-worker-0" || results[1] != "payload-from-worker-1"
	if !collision {
		t.Log("Note: In this particular scheduling tick, both workers saw their own writes, but collision is a fundamental risk of shared mutable paths.")
	} else {
		t.Logf("Proven collision in shared workspace: worker 0 saw %q, worker 1 saw %q", results[0], results[1])
	}
}

func TestJobExecutionTimeout_ExceedsTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "timeout-test-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:    "hung-job",
				Image:   "alpine:latest",
				Timeout: "50ms",
				Steps:   []drevtypes.Step{{Name: "sleep", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			select {
			case <-time.After(500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to return error due to timeout, got nil")
	}

	out := buf.String()
	expectedLog := `[drev] job "hung-job" timed out after 50ms`
	if !strings.Contains(out, expectedLog) {
		t.Errorf("expected log output to contain %q, got: %s", expectedLog, out)
	}

	// Verify run status in store is failed
	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusFailed, storedRun.Status)
	}

	// Verify job status in store is failed
	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusFailed {
		t.Errorf("expected stored job status %q, got %q", drevtypes.StatusFailed, jobs[0].Status)
	}
}

func TestJobExecutionTimeout_CompletesBeforeTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "fast-job-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:    "fast-job",
				Image:   "alpine:latest",
				Timeout: "1s",
				Steps:   []drevtypes.Step{{Name: "quick", Run: "echo quick"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err != nil {
		t.Fatalf("expected RunPipeline to succeed, got: %v", err)
	}

	// Verify run status in store is success
	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}

	// Verify job status in store is success
	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusSuccess {
		t.Errorf("expected stored job status %q, got %q", drevtypes.StatusSuccess, jobs[0].Status)
	}
}

func TestJobExecutionTimeout_NoTimeoutConfigured(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "no-timeout-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:  "standard-job",
				Image: "alpine:latest",
				Steps: []drevtypes.Step{{Name: "run", Run: "echo ok"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err != nil {
		t.Fatalf("expected RunPipeline to succeed, got: %v", err)
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}
}

func TestJobExecutionTimeout_WorkspaceCleanedUpOnTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "cleanup-on-timeout",
		Jobs: []drevtypes.Job{
			{
				Name:    "timed-out-job",
				Image:   "alpine:latest",
				Timeout: "50ms",
				Steps:   []drevtypes.Step{{Name: "sleep", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var capturedWorkspaceDir string
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			capturedWorkspaceDir = w.Dir
			// verify directory exists while running
			if _, err := os.Stat(capturedWorkspaceDir); err != nil {
				t.Errorf("expected workspace directory to exist during execution: %v", err)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	_ = sched.RunPipeline(context.Background(), pipeline, runID, &buf)

	if capturedWorkspaceDir == "" {
		t.Fatal("expected workspace dir to be captured")
	}

	// Workspace must be cleaned up after RunPipeline returns
	if _, err := os.Stat(capturedWorkspaceDir); !os.IsNotExist(err) {
		t.Errorf("expected workspace dir %q to be cleaned up after timeout, but it still exists", capturedWorkspaceDir)
	}
}

func TestJobExecutionTimeout_InvalidTimeoutHandledGracefully(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "invalid-timeout-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:    "bad-timeout-job",
				Image:   "alpine:latest",
				Timeout: "not-a-valid-duration",
				Steps:   []drevtypes.Step{{Name: "run", Run: "echo ok"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on invalid timeout, got nil")
	}

	out := buf.String()
	expectedLog := `[drev] invalid timeout "not-a-valid-duration" for job "bad-timeout-job"`
	if !strings.Contains(out, expectedLog) {
		t.Errorf("expected log output to contain %q, got: %s", expectedLog, out)
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusFailed, storedRun.Status)
	}
}

func TestJobExecutionTimeout_SuccessfulJobDoesNotFailIfTimerExpiresAfterCompletion(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "tight-timeout-success",
		Jobs: []drevtypes.Job{
			{
				Name:    "fast-finishing-job",
				Image:   "alpine:latest",
				Timeout: "25ms",
				Steps:   []drevtypes.Step{{Name: "run", Run: "echo done"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err != nil {
		t.Fatalf("expected successful job to remain success, got err: %v", err)
	}

	// Verify that even after the 25ms duration has passed, the job and run stay StatusSuccess
	time.Sleep(30 * time.Millisecond)

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusSuccess, storedRun.Status)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != drevtypes.StatusSuccess {
		t.Errorf("expected job status %q, got %q", drevtypes.StatusSuccess, jobs[0].Status)
	}
}

func TestTimeoutClassification_ParentDeadlineWithNoJobTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "parent-deadline-no-job-timeout",
		Jobs: []drevtypes.Job{
			{
				Name:  "no-timeout-job",
				Image: "alpine:latest",
				Steps: []drevtypes.Step{{Name: "run", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	parentCtx, parentCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer parentCancel()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to return error due to parent deadline, got nil")
	}

	out := buf.String()
	if strings.Contains(out, "timed out after") {
		t.Errorf("expected no job timeout message in log, got: %s", out)
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected stored run status %q, got %q", drevtypes.StatusFailed, storedRun.Status)
	}
}

func TestTimeoutClassification_ParentDeadlineWithConfiguredJobTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "parent-deadline-configured-job-timeout",
		Jobs: []drevtypes.Job{
			{
				Name:    "long-timeout-job",
				Image:   "alpine:latest",
				Timeout: "1h",
				Steps:   []drevtypes.Step{{Name: "run", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	parentCtx, parentCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer parentCancel()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected error due to parent deadline, got nil")
	}

	out := buf.String()
	if strings.Contains(out, `job "long-timeout-job" timed out after 1h`) {
		t.Errorf("parent deadline was falsely reported as a job timeout: %s", out)
	}
}

func TestTimeoutClassification_JobSpecificTimeoutWhileParentRemainsActive(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "job-timeout-active-parent",
		Jobs: []drevtypes.Job{
			{
				Name:    "hung-job",
				Image:   "alpine:latest",
				Timeout: "30ms",
				Steps:   []drevtypes.Step{{Name: "sleep", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected error due to job timeout, got nil")
	}

	out := buf.String()
	expectedLog := `[drev] job "hung-job" timed out after 30ms`
	if !strings.Contains(out, expectedLog) {
		t.Errorf("expected job timeout message %q in log, got: %s", expectedLog, out)
	}
}

func TestTimeoutClassification_SiblingCancellation(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "sibling-cancellation",
		Jobs: []drevtypes.Job{
			{
				Name:  "failing-job",
				Image: "alpine:latest",
				Steps: []drevtypes.Step{{Name: "fail", Run: "exit 1"}},
			},
			{
				Name:    "sibling-job",
				Image:   "alpine:latest",
				Timeout: "5m",
				Steps:   []drevtypes.Step{{Name: "run", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "failing-job" {
				return errors.New("command failed with exit code 1")
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected pipeline failure, got nil")
	}

	out := buf.String()
	if strings.Contains(out, `job "sibling-job" timed out after 5m`) {
		t.Errorf("cancelled sibling was falsely logged as timed out: %s", out)
	}
}

func TestTimeoutClassification_SiblingCancellationRacingWithJobTimeout(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "sibling-cancellation-race",
		Jobs: []drevtypes.Job{
			{
				Name:  "jobA",
				Image: "alpine:latest",
				Steps: []drevtypes.Step{{Name: "fail", Run: "exit 1"}},
			},
			{
				Name:    "jobB",
				Image:   "alpine:latest",
				Timeout: "5s",
				Steps:   []drevtypes.Step{{Name: "run", Run: "sleep 10"}},
			},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	jobACompleted := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "jobA" {
				defer close(jobACompleted)
				return errors.New("jobA failed with error")
			}
			if job.Name == "jobB" {
				// Deterministically synchronize: wait until jobA has returned its error and canceled egCtx
				<-jobACompleted
				<-ctx.Done()
				// Simulate jobB observing a deadline exceeded error concurrently
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected pipeline failure, got nil")
	}

	out := buf.String()
	// Because jobA failed and cancelled egCtx before jobB's timeout was evaluated,
	// jobB must NOT be reported as a job timeout.
	if strings.Contains(out, `job "jobB" timed out after`) {
		t.Errorf("jobB was falsely reported as a job timeout after sibling cancellation: %s", out)
	}
}
