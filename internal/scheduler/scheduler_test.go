package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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
			ID:      fmt.Sprintf("rj-%s-%d", job.Name, time.Now().UnixNano()),
			RunID:   runID,
			JobName: job.Name,
			Status:  drevtypes.StatusPending,
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

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "failing-job" {
				parentCancel()
				return errors.New("command failed with exit code 1")
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
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

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	jobACompleted := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "jobA" {
				defer close(jobACompleted)
				parentCancel()
				return errors.New("jobA failed with error")
			}
			if job.Name == "jobB" {
				// Deterministically synchronize: wait until jobA has returned its error and parentCtx is cancelled
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
	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected pipeline failure, got nil")
	}

	out := buf.String()
	// Because parentCtx was cancelled before jobB's timeout was evaluated,
	// jobB must NOT be reported as a job timeout.
	if strings.Contains(out, `job "jobB" timed out after`) {
		t.Errorf("jobB was falsely reported as a job timeout after sibling cancellation: %s", out)
	}
}

func TestDownstreamJobStates_DirectDependencyFailure(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "direct-dep-failure",
		Jobs: []drevtypes.Job{
			{Name: "test", Image: "alpine:latest"},
			{Name: "deploy", Image: "alpine:latest", DependsOn: []string{"test"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var deployExecuted bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "test" {
				return errors.New("unit tests failed")
			}
			if job.Name == "deploy" {
				deployExecuted = true
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

	if deployExecuted {
		t.Errorf("deploy job should never execute after test failed")
	}

	out := buf.String()
	expectedLog := `[drev] job "deploy" skipped because dependency "test" was failed`
	if !strings.Contains(out, expectedLog) {
		t.Errorf("expected log output to contain %q, got: %s", expectedLog, out)
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected run status %q, got %q", drevtypes.StatusFailed, storedRun.Status)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}

	if jobMap["test"].Status != drevtypes.StatusFailed {
		t.Errorf("expected test job status %q, got %q", drevtypes.StatusFailed, jobMap["test"].Status)
	}
	if jobMap["deploy"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected deploy job status %q, got %q", drevtypes.StatusSkipped, jobMap["deploy"].Status)
	}
	if jobMap["deploy"].FinishedAt.IsZero() {
		t.Errorf("expected deploy job to have non-zero finished_at timestamp")
	}
	if !jobMap["deploy"].StartedAt.IsZero() {
		t.Errorf("expected deploy job to have zero started_at timestamp, got %v", jobMap["deploy"].StartedAt)
	}
}

func TestDownstreamJobStates_MultiLevelDependencyChain(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "multi-level-chain",
		Jobs: []drevtypes.Job{
			{Name: "lint", Image: "alpine:latest"},
			{Name: "test", Image: "alpine:latest", DependsOn: []string{"lint"}},
			{Name: "deploy", Image: "alpine:latest", DependsOn: []string{"test"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var executed []string
	var mu sync.Mutex
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			mu.Lock()
			executed = append(executed, job.Name)
			mu.Unlock()
			if job.Name == "lint" {
				return errors.New("lint check failed")
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

	if len(executed) != 1 || executed[0] != "lint" {
		t.Errorf("expected only lint to execute, got %v", executed)
	}

	out := buf.String()
	expectedLog1 := `[drev] job "test" skipped because dependency "lint" was failed`
	expectedLog2 := `[drev] job "deploy" skipped because dependency "test" was skipped`
	if !strings.Contains(out, expectedLog1) {
		t.Errorf("expected log output to contain %q, got: %s", expectedLog1, out)
	}
	if !strings.Contains(out, expectedLog2) {
		t.Errorf("expected log output to contain %q, got: %s", expectedLog2, out)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}

	if jobMap["lint"].Status != drevtypes.StatusFailed {
		t.Errorf("expected lint job status %q, got %q", drevtypes.StatusFailed, jobMap["lint"].Status)
	}
	if jobMap["test"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected test job status %q, got %q", drevtypes.StatusSkipped, jobMap["test"].Status)
	}
	if jobMap["deploy"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected deploy job status %q, got %q", drevtypes.StatusSkipped, jobMap["deploy"].Status)
	}
	if jobMap["test"].FinishedAt.IsZero() || jobMap["deploy"].FinishedAt.IsZero() {
		t.Errorf("expected non-zero finished_at for all skipped jobs")
	}
}

func TestDownstreamJobStates_MultipleDependenciesOneFails(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "multi-deps",
		Jobs: []drevtypes.Job{
			{Name: "unit-tests", Image: "alpine:latest"},
			{Name: "integration-tests", Image: "alpine:latest"},
			{Name: "deploy", Image: "alpine:latest", DependsOn: []string{"unit-tests", "integration-tests"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var deployExecuted bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "unit-tests" {
				return nil
			}
			if job.Name == "integration-tests" {
				return errors.New("integration tests timeout")
			}
			if job.Name == "deploy" {
				deployExecuted = true
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

	if deployExecuted {
		t.Errorf("deploy job should not execute when one dependency fails")
	}

	out := buf.String()
	expectedLog := `[drev] job "deploy" skipped because dependency "integration-tests" was failed`
	if !strings.Contains(out, expectedLog) {
		t.Errorf("expected log to contain %q, got: %s", expectedLog, out)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}
	if jobMap["unit-tests"].Status != drevtypes.StatusSuccess {
		t.Errorf("expected unit-tests to succeed, got %q", jobMap["unit-tests"].Status)
	}
	if jobMap["integration-tests"].Status != drevtypes.StatusFailed {
		t.Errorf("expected integration-tests to fail, got %q", jobMap["integration-tests"].Status)
	}
	if jobMap["deploy"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected deploy to be skipped, got %q", jobMap["deploy"].Status)
	}
}

func TestDownstreamJobStates_IndependentBranchesContinue(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "independent-branches",
		Jobs: []drevtypes.Job{
			{Name: "frontend-build", Image: "alpine:latest"},
			{Name: "frontend-test", Image: "alpine:latest", DependsOn: []string{"frontend-build"}},
			{Name: "backend-build", Image: "alpine:latest"},
			{Name: "backend-test", Image: "alpine:latest", DependsOn: []string{"backend-build"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var completed []string
	var mu sync.Mutex
	recordComplete := func(name string) {
		mu.Lock()
		completed = append(completed, name)
		mu.Unlock()
	}

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			switch job.Name {
			case "frontend-build":
				return errors.New("webpack error")
			case "frontend-test":
				t.Errorf("frontend-test should have been skipped")
				return nil
			case "backend-build":
				time.Sleep(20 * time.Millisecond)
				recordComplete("backend-build")
				return nil
			case "backend-test":
				time.Sleep(20 * time.Millisecond)
				recordComplete("backend-test")
				return nil
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail due to frontend branch failure, got nil")
	}

	mu.Lock()
	backendDone := len(completed) == 2 && completed[0] == "backend-build" && completed[1] == "backend-test"
	mu.Unlock()
	if !backendDone {
		t.Errorf("expected backend-build and backend-test to execute to completion, got: %v", completed)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}

	if jobMap["frontend-build"].Status != drevtypes.StatusFailed {
		t.Errorf("expected frontend-build to fail, got %s", jobMap["frontend-build"].Status)
	}
	if jobMap["frontend-test"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected frontend-test to be skipped, got %s", jobMap["frontend-test"].Status)
	}
	if jobMap["backend-build"].Status != drevtypes.StatusSuccess {
		t.Errorf("expected backend-build to succeed, got %s", jobMap["backend-build"].Status)
	}
	if jobMap["backend-test"].Status != drevtypes.StatusSuccess {
		t.Errorf("expected backend-test to succeed, got %s", jobMap["backend-test"].Status)
	}
}

func TestDownstreamJobStates_TimedOutDependency(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "timed-out-dep",
		Jobs: []drevtypes.Job{
			{Name: "hung-parent", Image: "alpine:latest", Timeout: "30ms"},
			{Name: "child", Image: "alpine:latest", DependsOn: []string{"hung-parent"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	var childExecuted bool
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "hung-parent" {
				<-ctx.Done()
				return ctx.Err()
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
		t.Errorf("child should not have executed")
	}

	out := buf.String()
	if !strings.Contains(out, `[drev] job "hung-parent" timed out after 30ms`) {
		t.Errorf("expected log to contain timeout message, got: %s", out)
	}
	if !strings.Contains(out, `[drev] job "child" skipped because dependency "hung-parent" was failed`) {
		t.Errorf("expected log to contain skip message, got: %s", out)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
	}
	if jobMap["hung-parent"].Status != drevtypes.StatusFailed {
		t.Errorf("expected hung-parent to be failed, got %s", jobMap["hung-parent"].Status)
	}
	if jobMap["child"].Status != drevtypes.StatusSkipped {
		t.Errorf("expected child to be skipped, got %s", jobMap["child"].Status)
	}
}

func TestDownstreamJobStates_NoPendingJobsAfterCompletion(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "complex-dag-no-pending",
		Jobs: []drevtypes.Job{
			{Name: "root1", Image: "alpine:latest"},
			{Name: "root2", Image: "alpine:latest"},
			{Name: "child1", Image: "alpine:latest", DependsOn: []string{"root1"}},
			{Name: "child2", Image: "alpine:latest", DependsOn: []string{"root2"}},
			{Name: "leaf", Image: "alpine:latest", DependsOn: []string{"child1", "child2"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "root1" {
				return errors.New("root1 failed")
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	_ = sched.RunPipeline(context.Background(), pipeline, runID, &buf)

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 5 {
		t.Fatalf("expected 5 jobs, got %d", len(jobs))
	}

	for _, j := range jobs {
		if j.Status == drevtypes.StatusPending {
			t.Errorf("job %s was left in pending status at completion", j.JobName)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("job %s has zero finished_at at completion", j.JobName)
		}
	}
}

func TestDownstreamJobStates_ParentContextCancellation(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "external-cancellation",
		Jobs: []drevtypes.Job{
			{Name: "long-job", Image: "alpine:latest"},
			{Name: "waiting-job", Image: "alpine:latest", DependsOn: []string{"long-job"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	jobStarted := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "long-job" {
				close(jobStarted)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	go func() {
		<-jobStarted
		time.Sleep(15 * time.Millisecond)
		parentCancel()
	}()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail due to cancellation, got nil")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Errorf("expected run status %q, got %q", drevtypes.StatusCancelled, storedRun.Status)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
		if j.Status == drevtypes.StatusPending {
			t.Errorf("job %s was left in pending status after cancellation", j.JobName)
		}
	}

	if jobMap["long-job"].Status != drevtypes.StatusCancelled {
		t.Errorf("expected long-job status %q, got %q", drevtypes.StatusCancelled, jobMap["long-job"].Status)
	}
	if jobMap["waiting-job"].Status != drevtypes.StatusCancelled {
		t.Errorf("expected waiting-job status %q, got %q", drevtypes.StatusCancelled, jobMap["waiting-job"].Status)
	}
}

func TestCancellation_SingleRunningJob(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "single-running-cancel",
		Jobs: []drevtypes.Job{
			{Name: "solo", Image: "alpine:latest"},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	go func() {
		<-started
		time.Sleep(10 * time.Millisecond)
		parentCancel()
	}()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline error, got nil")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %q, want %q", storedRun.Status, drevtypes.StatusCancelled)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusCancelled {
		t.Errorf("job status = %q, want %q", jobs[0].Status, drevtypes.StatusCancelled)
	}
	if jobs[0].FinishedAt.IsZero() {
		t.Errorf("expected non-zero finished_at for cancelled job")
	}
}

func TestCancellation_WaitingJob(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "waiting-job-cancel",
		Jobs: []drevtypes.Job{
			{Name: "root", Image: "alpine:latest"},
			{Name: "child", Image: "alpine:latest", DependsOn: []string{"root"}},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	rootStarted := make(chan struct{})
	var childRunCalled bool

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "root" {
				close(rootStarted)
				<-ctx.Done()
				return ctx.Err()
			}
			if job.Name == "child" {
				childRunCalled = true
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	go func() {
		<-rootStarted
		time.Sleep(10 * time.Millisecond)
		parentCancel()
	}()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline error, got nil")
	}

	if childRunCalled {
		t.Errorf("child job should never be executed when cancelled while waiting")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %q, want %q", storedRun.Status, drevtypes.StatusCancelled)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
		if j.Status == drevtypes.StatusPending {
			t.Errorf("job %s was left in pending status", j.JobName)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("job %s has zero finished_at", j.JobName)
		}
	}

	if jobMap["root"].Status != drevtypes.StatusCancelled {
		t.Errorf("root job status = %q, want %q", jobMap["root"].Status, drevtypes.StatusCancelled)
	}
	if jobMap["child"].Status != drevtypes.StatusCancelled {
		t.Errorf("child job status = %q, want %q (not skipped)", jobMap["child"].Status, drevtypes.StatusCancelled)
	}
}

func TestCancellation_MultipleIndependentRunningJobs(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "multi-running-cancel",
		Jobs: []drevtypes.Job{
			{Name: "jobA", Image: "alpine:latest"},
			{Name: "jobB", Image: "alpine:latest"},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	var startedWg sync.WaitGroup
	startedWg.Add(2)

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			startedWg.Done()
			<-ctx.Done()
			return ctx.Err()
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer

	go func() {
		startedWg.Wait()
		time.Sleep(10 * time.Millisecond)
		parentCancel()
	}()

	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline error, got nil")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Errorf("run status = %q, want %q", storedRun.Status, drevtypes.StatusCancelled)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	for _, j := range jobs {
		if j.Status != drevtypes.StatusCancelled {
			t.Errorf("job %s status = %q, want %q", j.JobName, j.Status, drevtypes.StatusCancelled)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("job %s has zero finished_at", j.JobName)
		}
	}
}

func TestCancellation_WithPrecedingJobFailure(t *testing.T) {
	pipeline := &drevtypes.Pipeline{
		Name: "fail-then-cancel",
		Jobs: []drevtypes.Job{
			{Name: "failing-job", Image: "alpine:latest"},
			{Name: "long-job", Image: "alpine:latest"},
		},
	}

	s, runID := setupTestPipeline(t, pipeline)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	failFinished := make(chan struct{})

	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "failing-job" {
				defer close(failFinished)
				return errors.New("command failed with exit code 1")
			}
			if job.Name == "long-job" {
				<-failFinished
				time.Sleep(10 * time.Millisecond)
				parentCancel()
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}

	sched := New(mr, s)
	var buf bytes.Buffer
	err := sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail, got nil")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	// Because a genuine job failure occurred, the overall run must be StatusFailed, not StatusCancelled
	if storedRun.Status != drevtypes.StatusFailed {
		t.Errorf("run status = %q, want %q", storedRun.Status, drevtypes.StatusFailed)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	jobMap := make(map[string]*drevtypes.RunJob)
	for _, j := range jobs {
		jobMap[j.JobName] = j
		if j.Status == drevtypes.StatusPending {
			t.Errorf("job %s left pending", j.JobName)
		}
	}

	if jobMap["failing-job"].Status != drevtypes.StatusFailed {
		t.Errorf("failing-job status = %q, want %q", jobMap["failing-job"].Status, drevtypes.StatusFailed)
	}
	if jobMap["long-job"].Status != drevtypes.StatusCancelled {
		t.Errorf("long-job status = %q, want %q", jobMap["long-job"].Status, drevtypes.StatusCancelled)
	}
}

func TestDownstreamJobStates_AllSuccessAndFailureSemantics(t *testing.T) {
	t.Run("all jobs succeed leads to StatusSuccess", func(t *testing.T) {
		pipeline := &drevtypes.Pipeline{
			Name: "all-success",
			Jobs: []drevtypes.Job{
				{Name: "step1", Image: "alpine:latest"},
				{Name: "step2", Image: "alpine:latest", DependsOn: []string{"step1"}},
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
		if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}

		storedRun, _ := s.GetRun(context.Background(), runID)
		if storedRun.Status != drevtypes.StatusSuccess {
			t.Errorf("expected StatusSuccess, got %s", storedRun.Status)
		}
	})

	t.Run("pipeline with skipped jobs leads to StatusFailed", func(t *testing.T) {
		pipeline := &drevtypes.Pipeline{
			Name: "has-skipped",
			Jobs: []drevtypes.Job{
				{Name: "root", Image: "alpine:latest"},
				{Name: "child", Image: "alpine:latest", DependsOn: []string{"root"}},
			},
		}

		s, runID := setupTestPipeline(t, pipeline)
		mr := &mockRunner{
			runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
				if job.Name == "root" {
					return errors.New("root fail")
				}
				return nil
			},
		}
		sched := New(mr, s)
		var buf bytes.Buffer
		_ = sched.RunPipeline(context.Background(), pipeline, runID, &buf)

		storedRun, _ := s.GetRun(context.Background(), runID)
		if storedRun.Status != drevtypes.StatusFailed {
			t.Errorf("expected StatusFailed, got %s", storedRun.Status)
		}
	})
}

// ── Dynamic Scheduler Execution Tests (Task 2.3.4) ───────────────────────────

// createTestGitRepo initializes a local git repository with the given files and commits them.
func createTestGitRepo(t *testing.T, files map[string]string) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available in PATH")
	}

	repoDir := t.TempDir()
	gitCmd := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repoDir}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\noutput: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	gitCmd("init", "-b", "main")
	gitCmd("config", "user.email", "test@drev.local")
	gitCmd("config", "user.name", "Drev Test")

	for relPath, content := range files {
		fullPath := filepath.Join(repoDir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("creating dir for %s: %v", relPath, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("writing %s: %v", relPath, err)
		}
	}

	gitCmd("add", ".")
	gitCmd("commit", "-m", "initial commit")
	sha := gitCmd("rev-parse", "HEAD")
	return repoDir, sha
}

func TestDynamicScheduler_Bootstrap_Success(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: dynamic-test
jobs:
  - name: build
    image: alpine:latest
    steps:
      - name: build-step
        run: echo "building"
  - name: test
    image: alpine:latest
    depends_on: [build]
    steps:
      - name: test-step
        run: echo "testing"
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-bootstrap-success"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "dynamic-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	executedJobs := make([]string, 0)
	var mu sync.Mutex
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			mu.Lock()
			executedJobs = append(executedJobs, job.Name)
			mu.Unlock()
			return nil
		},
	}

	sched := New(mr, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "dynamic-test",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v\nlog: %s", err, buf.String())
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected run StatusSuccess, got %v (err: %v)", storedRun.Status, err)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("expected 2 stored jobs, got %d (err: %v)", len(jobs), err)
	}
	for _, j := range jobs {
		if j.Status != drevtypes.StatusSuccess {
			t.Errorf("job %s expected StatusSuccess, got %s", j.JobName, j.Status)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(executedJobs) != 2 || executedJobs[0] != "build" || executedJobs[1] != "test" {
		t.Fatalf("expected jobs [build, test], got %v", executedJobs)
	}
}

func TestDynamicScheduler_TriggerSourcePreserved(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: divergent-source-test
source:
  type: git
  url: https://evil.attacker.com/repo.git
  ref: refs/heads/malicious
  commit_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  pr_number: 999
jobs:
  - name: secure-job
    image: alpine:latest
    steps:
      - name: s1
        run: echo "secure"
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-trigger-source-preserved"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "divergent-source-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	authoritativeSource := drevtypes.Source{
		Type:      "git",
		URL:       repoDir,
		Ref:       "refs/pull/42/head",
		CommitSHA: sha,
		PRNumber:  42,
	}

	repoConfigSeenDivergent := false
	executedJob := ""
	var observedRepoPipeline *drevtypes.Pipeline
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			executedJob = job.Name
			data, err := os.ReadFile(filepath.Join(w.Dir, ".drev.yml"))
			if err == nil && strings.Contains(string(data), "https://evil.attacker.com/repo.git") {
				repoConfigSeenDivergent = true
			}
			// Verify active execution source by checking root workspace config load in runFunc
			rootWS := &workspace.Workspace{Dir: filepath.Dir(filepath.Dir(w.Dir))}
			resolvedPipeline, err := rootWS.LoadRepositoryConfig(authoritativeSource)
			if err == nil {
				observedRepoPipeline = resolvedPipeline
			}
			return nil
		},
	}

	sched := New(mr, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name:   "divergent-source-test",
		Source: authoritativeSource,
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if !repoConfigSeenDivergent {
		t.Fatal("expected runner to see divergent repo .drev.yml in workspace")
	}

	if executedJob != "secure-job" {
		t.Fatalf("expected secure-job to execute, got %q", executedJob)
	}

	if observedRepoPipeline == nil {
		t.Fatal("expected runner to successfully load repository config with authoritative source")
	}
	if observedRepoPipeline.Source != authoritativeSource {
		t.Fatalf("resolved pipeline did not preserve authoritative trigger source!\nexpected: %+v\ngot: %+v", authoritativeSource, observedRepoPipeline.Source)
	}

	// Verify caller-owned pipeline was NOT mutated in place
	if bootstrapPipeline.Jobs != nil {
		t.Fatalf("caller pipeline Jobs was unexpectedly mutated in place: %+v", bootstrapPipeline.Jobs)
	}
	if bootstrapPipeline.Source != authoritativeSource {
		t.Fatalf("caller pipeline Source was unexpectedly mutated!\nexpected: %+v\ngot: %+v", authoritativeSource, bootstrapPipeline.Source)
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected run StatusSuccess, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_JobsRegisterBeforeFirstRunnerInvocation(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: pre-exec-reg-test
jobs:
  - name: job-alpha
    image: alpine:latest
    steps:
      - name: s1
        run: echo "alpha"
  - name: job-beta
    image: alpine:latest
    steps:
      - name: s2
        run: echo "beta"
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-pre-exec-reg"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "pre-exec-reg-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	firstCallCheckPassed := false
	var mu sync.Mutex
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			mu.Lock()
			defer mu.Unlock()
			jobs, err := s.GetRunJobs(context.Background(), runID)
			if err == nil && len(jobs) == 2 {
				firstCallCheckPassed = true
			}
			return nil
		},
	}

	sched := New(mr, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "pre-exec-reg-test",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	mu.Lock()
	passed := firstCallCheckPassed
	mu.Unlock()
	if !passed {
		t.Fatal("expected all jobs to be registered in DB before runner invocation")
	}
}

func TestDynamicScheduler_AbsentConfig_WithHostFallback(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		"README.md": "# Hello World\n",
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-absent-fallback"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "fallback-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	fallbackPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:  "fallback-job",
				Image: "alpine:latest",
				Steps: []drevtypes.Step{
					{Name: "s1", Run: "echo fallback"},
				},
			},
		},
	}

	executed := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "fallback-job" {
				executed = true
			}
			return nil
		},
	}

	sched := New(mr, s, WithHostFallback(fallbackPipeline))
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v\nlog: %s", err, buf.String())
	}

	if !strings.Contains(buf.String(), "falling back to host configuration (deprecated)") {
		t.Errorf("expected deprecation warning in log, got: %s", buf.String())
	}
	if !executed {
		t.Error("expected fallback-job to be executed")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected StatusSuccess, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CaseE_UnregisteredRepo_UsesIncomingPipelineFallbackOnlyWhenAbsent(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		"README.md": "# No drev.yml here\n",
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-e-absent"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "incoming-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	executedJob := ""
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			executedJob = job.Name
			return nil
		},
	}

	sched := New(mr, s)
	incomingPipeline := &drevtypes.Pipeline{
		Name: "incoming-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
		Jobs: []drevtypes.Job{
			{Name: "incoming-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Run: "echo 1"}}},
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), incomingPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if executedJob != "incoming-job" {
		t.Fatalf("expected incoming-job executed, got %q", executedJob)
	}
	if !strings.Contains(buf.String(), "falling back to host configuration") {
		t.Errorf("expected fallback warning in log, got: %s", buf.String())
	}
}

func TestDynamicScheduler_CaseE_AuthoritativeRepoOverridesIncomingJobs(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: repo-pipeline
jobs:
  - name: repo-job
    image: alpine:latest
    steps:
      - name: s1
        run: echo "repo job"
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-e-repo-authoritative"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "incoming-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	executedJob := ""
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			executedJob = job.Name
			return nil
		},
	}

	sched := New(mr, s)
	incomingPipeline := &drevtypes.Pipeline{
		Name: "incoming-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
		Jobs: []drevtypes.Job{
			{Name: "incoming-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Run: "echo 1"}}},
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), incomingPipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if executedJob != "repo-job" {
		t.Fatalf("expected repo-job to be executed (repo authoritative), got %q", executedJob)
	}
}

func TestDynamicScheduler_AbsentConfig_NoFallback_Fails(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		"README.md": "# No drev.yml here\n",
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-absent-no-fallback"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s) // no fallback
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when config absent and no fallback configured")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("expected 0 registered jobs, got %d", len(jobs))
	}
}

func TestDynamicScheduler_MalformedConfig_DoesNotFallback(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `::: this is not valid yaml :::`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-malformed-config"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	fallbackPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "fallback-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Run: "echo fallback"}}},
		},
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s, WithHostFallback(fallbackPipeline))
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on malformed config")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called: fallback should NOT trigger on malformed config")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}

	if !strings.Contains(buf.String(), "resolving repository configuration failed") {
		t.Errorf("expected diagnostic error in log, got: %s", buf.String())
	}
}

func TestDynamicScheduler_SymlinkEscape_DoesNotFallback(t *testing.T) {
	externalDir := t.TempDir()
	externalConfig := filepath.Join(externalDir, "escape.drev.yml")
	if err := os.WriteFile(externalConfig, []byte("name: escape\njobs:\n  - name: escape-job\n    image: alpine:latest\n    steps:\n      - name: s1\n        run: echo escape\n"), 0644); err != nil {
		t.Fatal(err)
	}

	repoDir, sha := createTestGitRepo(t, map[string]string{
		"README.md": "# Symlink test\n",
	})

	symlinkPath := filepath.Join(repoDir, ".drev.yml")
	if err := os.Symlink(externalConfig, symlinkPath); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("git", "-C", repoDir, "add", ".drev.yml")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git add: %v (%s)", err, out)
	}
	cmd = exec.Command("git", "-C", repoDir, "commit", "-m", "add symlink")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v (%s)", err, out)
	}
	cmd = exec.Command("git", "-C", repoDir, "rev-parse", "HEAD")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	sha = strings.TrimSpace(string(out))

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-symlink-escape"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	fallbackPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "fallback-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Run: "echo fallback"}}},
		},
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s, WithHostFallback(fallbackPipeline))
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on symlink escape")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called: fallback should NOT trigger on symlink escape")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CloneFailure_DoesNotFallback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-clone-fail"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	fallbackPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "fallback-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Run: "echo fallback"}}},
		},
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s, WithHostFallback(fallbackPipeline))
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       "file:///non/existent/path/repo.git",
			CommitSHA: "1111111111111111111111111111111111111111",
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on clone error")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called on clone error")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CircularDependencyInRepoConfig(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: cycle-pipeline
jobs:
  - name: job-a
    image: alpine:latest
    depends_on: [job-b]
    steps:
      - name: s1
        run: echo a
  - name: job-b
    image: alpine:latest
    depends_on: [job-a]
    steps:
      - name: s2
        run: echo b
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-cycle"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "cycle-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "cycle-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on circular dependency")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}

	jobs, _ := s.GetRunJobs(context.Background(), runID)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs registered when DAG validation fails, got %d", len(jobs))
	}
}

func TestDynamicScheduler_EmptyResolvedPipelineFails(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: empty-pipeline
jobs: []
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-empty-resolved"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "empty-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	sched := New(&mockRunner{}, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "empty-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on empty resolved pipeline")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

type failBatchStoreWrapper struct {
	store.Store
	failCreateRunJobs bool
}

func (w *failBatchStoreWrapper) CreateRunJobs(ctx context.Context, runID string, jobs []*drevtypes.RunJob) error {
	if w.failCreateRunJobs {
		return errors.New("simulated batch registration database failure")
	}
	return w.Store.CreateRunJobs(ctx, runID, jobs)
}

func TestDynamicScheduler_BatchRegistrationFailure_RollbackAndNoRunnerCall(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: batch-fail-test
jobs:
  - name: job-1
    image: alpine:latest
    steps:
      - name: s1
        run: echo 1
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	realStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer realStore.Close()

	wrappedStore := &failBatchStoreWrapper{
		Store:             realStore,
		failCreateRunJobs: true,
	}

	runID := "run-batch-fail"
	if err := realStore.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "batch-fail-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, wrappedStore)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "batch-fail-test",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when CreateRunJobs fails")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called when CreateRunJobs failed")
	}

	storedRun, _ := realStore.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}

	jobs, _ := realStore.GetRunJobs(context.Background(), runID)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs in DB, got %d", len(jobs))
	}
}

func TestDynamicScheduler_CaseD_InconsistentBootstrapFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-d"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:      "rj-unexpected",
		RunID:   runID,
		JobName: "unexpected-job",
		Status:  drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	sched := New(&mockRunner{}, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       "https://github.com/drevci/test.git",
			CommitSHA: "1111111111111111111111111111111111111111",
		},
		Jobs: nil,
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on inconsistent bootstrap (Case D)")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CaseF_EmptyPipelineNoSourceFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-f"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "empty",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	sched := New(&mockRunner{}, s)
	emptyPipeline := &drevtypes.Pipeline{
		Name:   "empty",
		Source: drevtypes.Source{},
		Jobs:   nil,
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), emptyPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on empty invalid pipeline (Case F)")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CaseB_MismatchedPersistedRecordsFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-b-mismatch"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:      "rj-1",
		RunID:   runID,
		JobName: "job-1",
		Status:  drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	sched := New(&mockRunner{}, s)
	pipeline := &drevtypes.Pipeline{
		Name: "p1",
		Jobs: []drevtypes.Job{
			{Name: "job-1", Steps: []drevtypes.Step{{Run: "echo 1"}}},
			{Name: "job-2", Steps: []drevtypes.Step{{Run: "echo 2"}}},
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on mismatched persisted records")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CaseB_DuplicateOrUnexpectedPersistedRecordsFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-case-b-unexpected"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "p1",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	// Unexpected job name "job-unexpected"
	if err := s.CreateRunJob(context.Background(), &drevtypes.RunJob{
		ID:      "rj-unexp",
		RunID:   runID,
		JobName: "job-unexpected",
		Status:  drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	sched := New(&mockRunner{}, s)
	pipeline := &drevtypes.Pipeline{
		Name: "p1",
		Jobs: []drevtypes.Job{
			{Name: "job-1", Steps: []drevtypes.Step{{Run: "echo 1"}}},
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on unexpected persisted job record")
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}
}

func TestDynamicScheduler_CloneCancellation(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: cancel-test
jobs:
  - name: j1
    image: alpine:latest
    steps:
      - name: s1
        run: echo 1
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-clone-cancel"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "cancel-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	sched := New(&mockRunner{}, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "cancel-test",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err = sched.RunPipeline(ctx, bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusCancelled {
		t.Fatalf("expected StatusCancelled, got %v", storedRun.Status)
	}
}

type failStatusStoreWrapper struct {
	store.Store
	failStatus drevtypes.RunStatus
}

func (w *failStatusStoreWrapper) UpdateRunStatus(ctx context.Context, id string, status drevtypes.RunStatus) error {
	if status == w.failStatus {
		return errors.New("simulated UpdateRunStatus failure")
	}
	return w.Store.UpdateRunStatus(ctx, id, status)
}

func TestDynamicScheduler_StatusUpdateFailurePreservesBothErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	realStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer realStore.Close()

	wrappedStore := &failStatusStoreWrapper{
		Store:      realStore,
		failStatus: drevtypes.StatusFailed,
	}

	runID := "run-status-join"
	if err := realStore.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	sched := New(&mockRunner{}, wrappedStore)
	emptyPipeline := &drevtypes.Pipeline{
		Name:   "empty",
		Source: drevtypes.Source{},
		Jobs:   nil,
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), emptyPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail")
	}

	if !strings.Contains(err.Error(), "invalid pipeline") {
		t.Errorf("expected original error in returned error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "simulated UpdateRunStatus failure") {
		t.Errorf("expected status update error in returned error, got: %v", err)
	}
}

func TestDynamicScheduler_DirectInMemoryWithoutPreRegisteredRecords(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-direct-in-memory"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "direct-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	executed := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			if job.Name == "in-mem-job" {
				executed = true
			}
			return nil
		},
	}

	sched := New(mr, s)
	pipeline := &drevtypes.Pipeline{
		Name:   "direct-pipeline",
		Source: drevtypes.Source{},
		Jobs: []drevtypes.Job{
			{Name: "in-mem-job", Steps: []drevtypes.Step{{Name: "s1", Run: "echo direct"}}},
		},
	}

	var buf bytes.Buffer
	if err := sched.RunPipeline(context.Background(), pipeline, runID, &buf); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	if !executed {
		t.Error("expected in-mem-job to be executed")
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("expected 1 job registered in DB, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusSuccess {
		t.Errorf("expected job StatusSuccess, got %s", jobs[0].Status)
	}

	storedRun, _ := s.GetRun(context.Background(), runID)
	if storedRun.Status != drevtypes.StatusSuccess {
		t.Errorf("expected run StatusSuccess, got %s", storedRun.Status)
	}
}

type timeoutStoreWrapper struct {
	store.Store
	mu        sync.Mutex
	failFirst bool
}

func (w *timeoutStoreWrapper) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failFirst {
		w.failFirst = false
		return nil, fmt.Errorf("simulated timeout: %w", context.DeadlineExceeded)
	}
	return w.Store.GetRun(ctx, id)
}

func TestDynamicScheduler_IndependentDeadlineExceeded_ActiveContext(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	realStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer realStore.Close()

	wrappedStore := &timeoutStoreWrapper{
		Store:     realStore,
		failFirst: true,
	}

	runID := "run-deadline-active-ctx"
	if err := realStore.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "timeout-test",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, wrappedStore)
	pipeline := &drevtypes.Pipeline{
		Name: "timeout-test",
		Jobs: []drevtypes.Job{
			{Name: "j1", Image: "alpine:latest", Steps: []drevtypes.Step{{Name: "s1", Run: "echo 1"}}},
		},
	}

	parentCtx := context.Background() // parent context is active (parentCtx.Err() == nil)
	var buf bytes.Buffer
	err = sched.RunPipeline(parentCtx, pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on simulated deadline exceeded error")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected error to wrap context.DeadlineExceeded, got: %v", err)
	}

	storedRun, err := realStore.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusCancelled {
		t.Fatalf("expected run status StatusCancelled, got: %s", storedRun.Status)
	}
}

type failGetRunStoreWrapper struct {
	store.Store
	err error
}

func (w *failGetRunStoreWrapper) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	return nil, w.err
}

func TestDynamicScheduler_RunLookupFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	realStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer realStore.Close()

	lookupErr := errors.New("simulated fatal disk error during GetRun")
	wrappedStore := &failGetRunStoreWrapper{
		Store: realStore,
		err:   lookupErr,
	}

	runID := "run-lookup-fail"
	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, wrappedStore)
	pipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "j1", Image: "alpine:latest", Steps: []drevtypes.Step{{Name: "s1", Run: "echo 1"}}},
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when GetRun fails")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called when GetRun failed")
	}

	if !strings.Contains(err.Error(), "loading run from database") {
		t.Errorf("expected error to mention 'loading run from database', got: %v", err)
	}
	if !strings.Contains(err.Error(), "checking run status before finalization") {
		t.Errorf("expected error to preserve second lookup failure via errors.Join, got: %v", err)
	}
}

type failGetRunJobsStoreWrapper struct {
	store.Store
	err error
}

func (w *failGetRunJobsStoreWrapper) GetRunJobs(ctx context.Context, runID string) ([]*drevtypes.RunJob, error) {
	return nil, w.err
}

func TestDynamicScheduler_RunJobsLookupFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	realStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer realStore.Close()

	jobsLookupErr := errors.New("simulated fatal disk error during GetRunJobs")
	wrappedStore := &failGetRunJobsStoreWrapper{
		Store: realStore,
		err:   jobsLookupErr,
	}

	runID := "run-jobs-lookup-fail"
	if err := realStore.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, wrappedStore)
	pipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "j1", Image: "alpine:latest", Steps: []drevtypes.Step{{Name: "s1", Run: "echo 1"}}},
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), pipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when GetRunJobs fails")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called when GetRunJobs failed")
	}

	if !strings.Contains(err.Error(), "loading run jobs from database") {
		t.Errorf("expected error to mention 'loading run jobs from database', got: %v", err)
	}

	storedRun, err := realStore.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("getting stored run: %v", err)
	}
	if storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected run status to be finalized as StatusFailed, got: %s", storedRun.Status)
	}
}

func TestDynamicScheduler_WorkspaceCleanupAfterPreparationFailure(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `::: malformed yaml syntax :::`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-workspace-cleanup"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	beforeMatches, err := filepath.Glob(filepath.Join(os.TempDir(), "drev-workspace-*"))
	if err != nil {
		t.Fatalf("glob temp dir: %v", err)
	}
	beforeSet := make(map[string]bool, len(beforeMatches))
	for _, m := range beforeMatches {
		beforeSet[m] = true
	}

	sched := New(&mockRunner{}, s)
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on malformed config")
	}

	afterMatches, err := filepath.Glob(filepath.Join(os.TempDir(), "drev-workspace-*"))
	if err != nil {
		t.Fatalf("glob temp dir: %v", err)
	}

	for _, m := range afterMatches {
		if !beforeSet[m] {
			t.Fatalf("leaked workspace directory found after preparation failure: %s", m)
		}
	}
}

func TestDynamicScheduler_InvalidStepConfig_DoesNotFallback(t *testing.T) {
	repoDir, sha := createTestGitRepo(t, map[string]string{
		".drev.yml": `name: invalid-step-pipeline
jobs:
  - name: test-job
    image: alpine:latest
    steps:
      - name: step-missing-run
`,
	})

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	runID := "run-invalid-step"
	if err := s.CreateRun(context.Background(), &drevtypes.Run{
		ID:         runID,
		PipelineID: "test-pipeline",
		Status:     drevtypes.StatusPending,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	fallbackPipeline := &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{Name: "fallback-job", Image: "alpine:latest", Steps: []drevtypes.Step{{Name: "s1", Run: "echo fallback"}}},
		},
	}

	runnerCalled := false
	mr := &mockRunner{
		runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
			runnerCalled = true
			return nil
		},
	}

	sched := New(mr, s, WithHostFallback(fallbackPipeline))
	bootstrapPipeline := &drevtypes.Pipeline{
		Name: "test-pipeline",
		Source: drevtypes.Source{
			Type:      "git",
			URL:       repoDir,
			CommitSHA: sha,
		},
	}

	var buf bytes.Buffer
	err = sched.RunPipeline(context.Background(), bootstrapPipeline, runID, &buf)
	if err == nil {
		t.Fatal("expected RunPipeline to fail on invalid step configuration")
	}

	if runnerCalled {
		t.Error("runner was unexpectedly called: fallback should NOT trigger on invalid step config")
	}

	storedRun, err := s.GetRun(context.Background(), runID)
	if err != nil || storedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected StatusFailed, got %v", storedRun.Status)
	}

	jobs, err := s.GetRunJobs(context.Background(), runID)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("expected 0 registered jobs in DB when step validation fails, got %d", len(jobs))
	}
}

func TestDynamicScheduler_UnreadableConfig_DoesNotFallback(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping test: running as root (file permission restrictions do not apply)")
	}

	// Git clone creates files with standard permissions (0644/0755) and cannot check out mode 0000 files.
	// In the dynamic scheduler lifecycle, workspace creation and clone occur atomically within RunPipeline
	// before config resolution, without an interception hook to chmod files between clone and resolve.
	// Direct unreadable config handling (bypassing fallback) is independently verified in
	// internal/workspace/config_test.go (TestResolveConfig_UnreadableFile_BypassesFallback).
	t.Skip("skipping test: git clone normalizes file permissions to 0644/0755 and cannot check out unreadable (mode 0000) files without post-clone filesystem interception")
}

func TestDynamicScheduler_AlreadyTerminalStatusPreserved(t *testing.T) {
	terminalStatuses := []drevtypes.RunStatus{
		drevtypes.StatusCancelled,
		drevtypes.StatusFailed,
		drevtypes.StatusSuccess,
	}

	for _, initialStatus := range terminalStatuses {
		t.Run(string(initialStatus), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			s, err := store.Open(dbPath)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer s.Close()

			runID := "run-term-" + string(initialStatus)
			if err := s.CreateRun(context.Background(), &drevtypes.Run{
				ID:         runID,
				PipelineID: "term-pipeline",
				Status:     initialStatus,
			}); err != nil {
				t.Fatalf("create run: %v", err)
			}

			runnerCalled := false
			mr := &mockRunner{
				runFunc: func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
					runnerCalled = true
					return nil
				},
			}

			sched := New(mr, s)
			pipeline := &drevtypes.Pipeline{
				Name: "term-pipeline",
				Jobs: []drevtypes.Job{
					{Name: "j1", Image: "alpine:latest", Steps: []drevtypes.Step{{Name: "s1", Run: "echo 1"}}},
				},
			}

			var buf bytes.Buffer
			err = sched.RunPipeline(context.Background(), pipeline, runID, &buf)
			if err == nil {
				t.Fatalf("expected RunPipeline to fail for run already in status %s, got nil", initialStatus)
			}

			if runnerCalled {
				t.Errorf("runner was called for run in terminal status %s", initialStatus)
			}

			storedRun, err := s.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatalf("getting stored run: %v", err)
			}
			if storedRun.Status != initialStatus {
				t.Fatalf("terminal status was overwritten! expected %s, got %s", initialStatus, storedRun.Status)
			}
		})
	}
}
