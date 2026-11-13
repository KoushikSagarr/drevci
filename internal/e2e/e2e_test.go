package e2e_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/internal/notify"
	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/pool"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// ── Test Helpers & Infrastructure ──────────────────────────────────────────

func signWebhookBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func createTestGitRepo(t *testing.T, files map[string]string) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repoDir := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatalf("creating repo dir: %v", err)
	}

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
	gitCmd("config", "user.name", "Drev E2E Test")

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

func createPRBranch(t *testing.T, repoDir, branchName string, files map[string]string) string {
	t.Helper()
	gitCmd := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repoDir}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\noutput: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	gitCmd("checkout", "-b", branchName)
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
	gitCmd("commit", "-m", "pr commit on "+branchName)
	sha := gitCmd("rev-parse", "HEAD")
	// Switch back to main so origin repo HEAD on disk is main
	gitCmd("checkout", "main")
	return sha
}

type mockJobRunner struct {
	mu            sync.Mutex
	executedJobs  []string
	jobWorkspaces map[string]string
	runJobFunc    func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error
}

func (m *mockJobRunner) RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
	m.mu.Lock()
	m.executedJobs = append(m.executedJobs, job.Name)
	if m.jobWorkspaces == nil {
		m.jobWorkspaces = make(map[string]string)
	}
	if w != nil {
		m.jobWorkspaces[job.Name] = w.Dir
	}
	customFn := m.runJobFunc
	m.mu.Unlock()

	if logWriter != nil {
		fmt.Fprintf(logWriter, "[runner] executing job: %s\n", job.Name)
	}

	if customFn != nil {
		return customFn(ctx, run, job, w, logWriter)
	}
	return nil
}

func (m *mockJobRunner) getExecutedJobs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]string, len(m.executedJobs))
	copy(res, m.executedJobs)
	return res
}

func (m *mockJobRunner) getWorkspace(jobName string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobWorkspaces[jobName]
}

func waitForRunStatus(ctx context.Context, t *testing.T, s store.Store, runID string, wantStatus drevtypes.RunStatus, timeout time.Duration) *drevtypes.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := s.GetRun(ctx, runID)
		if err == nil && run != nil {
			if run.Status == wantStatus {
				return run
			}
			if (run.Status == drevtypes.StatusSuccess || run.Status == drevtypes.StatusFailed || run.Status == drevtypes.StatusCancelled) && run.Status != wantStatus {
				t.Fatalf("run %s reached unexpected terminal status %q (wanted %q)", runID, run.Status, wantStatus)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled while waiting for run %s: %v", runID, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("timed out after %s waiting for run %s to reach status %q", timeout, runID, wantStatus)
	return nil
}

type e2eHarness struct {
	store     store.Store
	queue     *queue.Queue
	streamer  *streamer.LogStreamer
	runner    *mockJobRunner
	scheduler *scheduler.Scheduler
	pool      *pool.Pool
	webhook   *webhook.GitHubHandler
	secret    string
}

func setupE2EHarness(t *testing.T, customStore store.Store) *e2eHarness {
	t.Helper()

	s := customStore
	if s == nil {
		dbPath := filepath.Join(t.TempDir(), "e2e.db")
		sqliteStore, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		t.Cleanup(func() { sqliteStore.Close() })
		s = sqliteStore
	}

	q := queue.New(10)
	stream := streamer.New(t.TempDir())
	mr := &mockJobRunner{}
	sched := scheduler.New(mr, s)
	notif := notify.New(notify.NotificationConfig{Enabled: false})
	wp := pool.New(2, q, sched, s, notif)

	secret := "test-e2e-webhook-secret"
	wh := webhook.New(s, q, parser.NewParser(), stream, secret, t.TempDir())

	t.Cleanup(func() {
		// Drain queue and close writers to avoid file lock leaks
		for {
			select {
			case job := <-q.Drain():
				if job.LogWriter != nil {
					job.LogWriter.Close()
				}
			default:
				return
			}
		}
	})

	return &e2eHarness{
		store:     s,
		queue:     q,
		streamer:  stream,
		runner:    mr,
		scheduler: sched,
		pool:      wp,
		webhook:   wh,
		secret:    secret,
	}
}

// ── Test A: Push Webhook to Successful Dynamic Execution ───────────────────

func TestE2E_PushWebhook_To_SuccessfulDynamicExecution(t *testing.T) {
	// 1. Create a local Git repository with two dependent jobs in .drev.yml
	pipelineYAML := `name: e2e-push-pipeline
jobs:
  - name: build
    image: alpine:latest
    steps:
      - name: compile
        run: echo "compiling"
  - name: test
    image: alpine:latest
    depends_on: [build]
    steps:
      - name: run-tests
        run: echo "testing"
`
	repoDir, headSHA := createTestGitRepo(t, map[string]string{
		".drev.yml":   pipelineYAML,
		"src/main.go": "package main\nfunc main() {}\n",
		"README.md":   "# E2E Test Repo\n",
	})

	// 2. Set up full engine harness
	h := setupE2EHarness(t, nil)

	// Mock runner verifies that cloned files exist in the job's workspace
	h.runner.runJobFunc = func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
		if w == nil {
			return fmt.Errorf("job %s received nil workspace", job.Name)
		}
		appFile := filepath.Join(w.Dir, "src", "main.go")
		if _, err := os.Stat(appFile); err != nil {
			return fmt.Errorf("job %s missing checked-out src/main.go in workspace %s: %w", job.Name, w.Dir, err)
		}
		cfgFile := filepath.Join(w.Dir, ".drev.yml")
		if _, err := os.Stat(cfgFile); err != nil {
			return fmt.Errorf("job %s missing .drev.yml in workspace: %w", job.Name, err)
		}
		return nil
	}

	// 3. Construct and deliver signed push webhook HTTP request
	payload := webhook.PushPayload{
		Ref: "refs/heads/main",
	}
	payload.Repository.FullName = "acme/push-e2e"
	payload.Repository.CloneURL = repoDir
	payload.HeadCommit.ID = headSHA
	payload.HeadCommit.Message = "feat: add push pipeline"
	payload.Pusher.Name = "developer"
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshaling push payload: %v", err)
	}

	deliveryID := "del-push-e2e-001"
	req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(h.secret, bodyBytes))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.webhook.Handle(rr, req)

	// 4. Verify HTTP 202 Accepted
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response json: %v", err)
	}
	runID, ok := resp["run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("response missing run_id: %+v", resp)
	}
	if resp["commit"] != headSHA[:7] {
		t.Errorf("expected short commit %s, got %v", headSHA[:7], resp["commit"])
	}

	ctx := context.Background()

	// 5. Verify initial database state before worker processing:
	//    - Run is StatusPending
	//    - ZERO pre-registered run jobs
	//    - Durable outbox entry exists with exact bootstrap metadata
	initialRun, err := h.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("store.GetRun: %v", err)
	}
	if initialRun.Status != drevtypes.StatusPending {
		t.Errorf("expected initial run status pending, got %s", initialRun.Status)
	}
	if initialRun.CommitSHA != headSHA {
		t.Errorf("expected run commit SHA %s, got %s", headSHA, initialRun.CommitSHA)
	}

	initialJobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("store.GetRunJobs: %v", err)
	}
	if len(initialJobs) != 0 {
		t.Fatalf("expected 0 jobs in DB at ingestion, got %d", len(initialJobs))
	}

	outboxItems, err := h.store.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("store.GetPendingOutbox: %v", err)
	}
	if len(outboxItems) != 1 || outboxItems[0].RunID != runID {
		t.Fatalf("expected 1 outbox item for run %s, got %+v", runID, outboxItems)
	}

	var outboxSpec drevtypes.Pipeline
	if err := json.Unmarshal([]byte(outboxItems[0].PipelineSpec), &outboxSpec); err != nil {
		t.Fatalf("unmarshaling outbox spec: %v", err)
	}
	if len(outboxSpec.Jobs) != 0 {
		t.Errorf("expected 0 jobs in outbox bootstrap spec, got %d", len(outboxSpec.Jobs))
	}
	if outboxSpec.Source.CommitSHA != headSHA {
		t.Errorf("expected outbox CommitSHA %s, got %s", headSHA, outboxSpec.Source.CommitSHA)
	}
	if outboxSpec.Source.Ref != "main" {
		t.Errorf("expected outbox Ref main, got %s", outboxSpec.Source.Ref)
	}
	if outboxSpec.Source.URL != repoDir {
		t.Errorf("expected outbox URL %s, got %s", repoDir, outboxSpec.Source.URL)
	}

	// 6. Start the real worker pool to process the queue job
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()
	t.Cleanup(func() {
		poolCancel()
		h.pool.Stop()
	})
	h.pool.Start(poolCtx)

	// 7. Wait for run to reach terminal StatusSuccess
	finalRun := waitForRunStatus(poolCtx, t, h.store, runID, drevtypes.StatusSuccess, 10*time.Second)
	if finalRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected final run status success, got %s", finalRun.Status)
	}
	if finalRun.StartedAt.IsZero() {
		t.Errorf("expected run StartedAt to be populated")
	}
	if finalRun.FinishedAt.IsZero() {
		t.Errorf("expected run FinishedAt to be populated")
	}

	// 8. Verify actual database state after completion:
	//    - Exactly 2 jobs batch-registered and both StatusSuccess
	//    - Delivery record intact
	//    - Outbox entry removed upon claim
	finalJobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("store.GetRunJobs: %v", err)
	}
	if len(finalJobs) != 2 {
		t.Fatalf("expected 2 jobs registered in DB, got %d", len(finalJobs))
	}
	jobStatusMap := make(map[string]drevtypes.RunStatus)
	for _, j := range finalJobs {
		jobStatusMap[j.JobName] = j.Status
		if j.StartedAt.IsZero() {
			t.Errorf("job %s missing StartedAt", j.JobName)
		}
		if j.FinishedAt.IsZero() {
			t.Errorf("job %s missing FinishedAt", j.JobName)
		}
	}
	if jobStatusMap["build"] != drevtypes.StatusSuccess {
		t.Errorf("expected job 'build' status success, got %s", jobStatusMap["build"])
	}
	if jobStatusMap["test"] != drevtypes.StatusSuccess {
		t.Errorf("expected job 'test' status success, got %s", jobStatusMap["test"])
	}

	del, err := h.store.GetDelivery(ctx, deliveryID)
	if err != nil || del == nil {
		t.Fatalf("expected delivery record %s in DB, err: %v", deliveryID, err)
	}
	if del.RunID != runID {
		t.Errorf("delivery.RunID %s != runID %s", del.RunID, runID)
	}

	remainingOutbox, err := h.store.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("store.GetPendingOutbox: %v", err)
	}
	if len(remainingOutbox) != 0 {
		t.Errorf("expected outbox entry to be deleted after claim, found %d items", len(remainingOutbox))
	}

	// 9. Verify DAG execution order through mock runner
	executed := h.runner.getExecutedJobs()
	if len(executed) != 2 {
		t.Fatalf("expected 2 executed jobs, got %v", executed)
	}
	if executed[0] != "build" || executed[1] != "test" {
		t.Errorf("expected DAG execution order [build, test], got %v", executed)
	}

	// 10. Verify workspace directory recorded by runner had the checked-out files
	buildWs := h.runner.getWorkspace("build")
	if buildWs == "" {
		t.Errorf("expected workspace directory recorded for 'build'")
	}
}

// ── Test B: Pull Request Webhook to Successful Dynamic Execution ───────────

func TestE2E_PullRequestWebhook_To_SuccessfulDynamicExecution(t *testing.T) {
	// 1. Create a local Git repository on main
	repoDir, mainSHA := createTestGitRepo(t, map[string]string{
		"README.md": "# Main branch\n",
	})

	// 2. Create a PR branch 'feature/pr-dynamic' with its own .drev.yml and files
	prPipelineYAML := `name: e2e-pr-pipeline
jobs:
  - name: lint
    image: alpine:latest
    steps:
      - name: check
        run: echo "linting"
  - name: build
    image: alpine:latest
    depends_on: [lint]
    steps:
      - name: compile
        run: echo "building"
`
	prHeadSHA := createPRBranch(t, repoDir, "feature/pr-dynamic", map[string]string{
		".drev.yml":      prPipelineYAML,
		"src/feature.go": "package feature\n// PR specific code\n",
		"pr_marker.txt":  "pr-feature-verified",
	})

	if prHeadSHA == mainSHA {
		t.Fatalf("PR head SHA should differ from main SHA")
	}

	// 3. Set up integration harness
	h := setupE2EHarness(t, nil)

	// Mock runner asserts that the checked-out workspace contains the PR branch files
	h.runner.runJobFunc = func(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
		markerFile := filepath.Join(w.Dir, "pr_marker.txt")
		content, err := os.ReadFile(markerFile)
		if err != nil {
			return fmt.Errorf("job %s: PR marker file missing in workspace %s: %w", job.Name, w.Dir, err)
		}
		if string(content) != "pr-feature-verified" {
			return fmt.Errorf("job %s: unexpected PR marker content: %q", job.Name, string(content))
		}
		return nil
	}

	// 4. Construct and deliver signed PR webhook HTTP request
	prNum := 77
	payload := webhook.PullRequestPayload{
		Action: "opened",
		Number: prNum,
	}
	payload.Repository.FullName = "acme/pr-e2e"
	payload.Repository.CloneURL = repoDir
	payload.PullRequest.Number = prNum
	payload.PullRequest.Title = "Add awesome feature"
	payload.PullRequest.User.Login = "pr-author"
	payload.PullRequest.Head.Ref = "feature/pr-dynamic"
	payload.PullRequest.Head.SHA = prHeadSHA
	payload.PullRequest.Head.Repo.FullName = "acme/pr-e2e"
	payload.PullRequest.Head.Repo.CloneURL = repoDir
	payload.PullRequest.Head.Repo.Fork = false
	payload.PullRequest.Base.Repo.FullName = "acme/pr-e2e"

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshaling pr payload: %v", err)
	}

	deliveryID := "del-pr-e2e-001"
	req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(h.secret, bodyBytes))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.webhook.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	runID := resp["run_id"].(string)

	ctx := context.Background()

	// 5. Verify initial DB state:
	//    - Zero pre-registered jobs
	//    - Outbox contains PRNumber and PR Head SHA
	initialJobs, _ := h.store.GetRunJobs(ctx, runID)
	if len(initialJobs) != 0 {
		t.Fatalf("expected 0 jobs at PR ingestion, got %d", len(initialJobs))
	}

	outboxItems, _ := h.store.GetPendingOutbox(ctx, 10)
	if len(outboxItems) != 1 {
		t.Fatalf("expected 1 outbox item, got %d", len(outboxItems))
	}
	var outboxSpec drevtypes.Pipeline
	json.Unmarshal([]byte(outboxItems[0].PipelineSpec), &outboxSpec)
	if outboxSpec.Source.PRNumber != prNum {
		t.Errorf("expected outbox PRNumber %d, got %d", prNum, outboxSpec.Source.PRNumber)
	}
	if outboxSpec.Source.CommitSHA != prHeadSHA {
		t.Errorf("expected outbox CommitSHA %s, got %s", prHeadSHA, outboxSpec.Source.CommitSHA)
	}

	// 6. Start pool worker
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()
	t.Cleanup(func() {
		poolCancel()
		h.pool.Stop()
	})
	h.pool.Start(poolCtx)

	// 7. Wait for terminal status
	finalRun := waitForRunStatus(poolCtx, t, h.store, runID, drevtypes.StatusSuccess, 10*time.Second)
	if finalRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected final run status success, got %s", finalRun.Status)
	}

	// 8. Verify DB state:
	//    - 2 jobs (lint, build) both StatusSuccess
	//    - Outbox cleared
	finalJobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(finalJobs) != 2 {
		t.Fatalf("expected 2 jobs registered for PR pipeline, got %d", len(finalJobs))
	}
	for _, j := range finalJobs {
		if j.Status != drevtypes.StatusSuccess {
			t.Errorf("job %s status %s, want success", j.JobName, j.Status)
		}
	}

	outboxRemaining, _ := h.store.GetPendingOutbox(ctx, 10)
	if len(outboxRemaining) != 0 {
		t.Errorf("expected outbox to be cleared after PR claim, got %d", len(outboxRemaining))
	}

	// 9. Verify DAG execution order [lint, build]
	executed := h.runner.getExecutedJobs()
	if len(executed) != 2 || executed[0] != "lint" || executed[1] != "build" {
		t.Errorf("expected execution order [lint, build], got %v", executed)
	}
}

// ── Test C: Outbox Recovery Crash-Restart Execution ─────────────────────────

func TestE2E_OutboxRecovery_CrashRestart_Execution(t *testing.T) {
	// 1. Create a local Git repo with valid dynamic pipeline
	pipelineYAML := `name: e2e-recovery-pipeline
jobs:
  - name: step-1
    image: alpine:latest
    steps:
      - name: run-step-1
        run: echo "step 1"
  - name: step-2
    image: alpine:latest
    depends_on: [step-1]
    steps:
      - name: run-step-2
        run: echo "step 2"
`
	repoDir, headSHA := createTestGitRepo(t, map[string]string{
		".drev.yml":   pipelineYAML,
		"src/code.go": "package code\n",
	})

	dbPath := filepath.Join(t.TempDir(), "recovery_e2e.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	secret := "recovery-webhook-secret"

	// 2. Set up initial queue & streamer, but DO NOT START ANY WORKER POOL
	//    This simulates webhook arriving just before the daemon crashes / restarts
	initialQueue := queue.New(10)
	initialStreamer := streamer.New(t.TempDir())
	wh := webhook.New(s, initialQueue, parser.NewParser(), initialStreamer, secret, t.TempDir())

	// Deliver push webhook
	payload := webhook.PushPayload{
		Ref: "refs/heads/main",
	}
	payload.Repository.FullName = "acme/recovery-e2e"
	payload.Repository.CloneURL = repoDir
	payload.HeadCommit.ID = headSHA
	payload.Pusher.Name = "crashtest"
	bodyBytes, _ := json.Marshal(payload)

	deliveryID := "del-recovery-001"
	req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, bodyBytes))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	wh.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	runID := resp["run_id"].(string)

	ctx := context.Background()

	// Verify run is pending in DB and outbox has 1 item
	initialRun, _ := s.GetRun(ctx, runID)
	if initialRun.Status != drevtypes.StatusPending {
		t.Fatalf("expected run status pending, got %s", initialRun.Status)
	}
	outboxItems, _ := s.GetPendingOutbox(ctx, 10)
	if len(outboxItems) != 1 {
		t.Fatalf("expected 1 outbox item, got %d", len(outboxItems))
	}

	// 3. SIMULATE DAEMON CRASH: Discard initialQueue (all in-memory state is lost)
	//    Safely close abandoned log writers from the crashed process to release file handles.
drainInitial:
	for {
		select {
		case abandonedJob := <-initialQueue.Drain():
			if abandonedJob.LogWriter != nil {
				_ = abandonedJob.LogWriter.Close()
			}
		default:
			break drainInitial
		}
	}

	//    Now simulate daemon startup:
	//    - Fresh in-memory queue
	//    - Fresh streamer
	//    - OutboxRecoverer recovers pending runs into the new queue
	freshQueue := queue.New(10)
	freshStreamer := streamer.New(t.TempDir())
	mr := &mockJobRunner{}
	sched := scheduler.New(mr, s)
	freshPool := pool.New(1, freshQueue, sched, s, nil)

	recoverer := pool.NewOutboxRecoverer(s, freshQueue, freshStreamer)
	if err := recoverer.Recover(ctx); err != nil {
		t.Fatalf("OutboxRecoverer.Recover failed: %v", err)
	}

	// Verify that freshQueue received exactly 1 recovered job
	if freshQueue.Depth() != 1 {
		t.Fatalf("expected 1 job recovered into fresh queue, got %d", freshQueue.Depth())
	}

	// 4. Start the recovered worker pool
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()
	t.Cleanup(func() {
		poolCancel()
		freshPool.Stop()
	})
	freshPool.Start(poolCtx)

	// 5. Wait for the recovered run to complete with StatusSuccess
	finalRun := waitForRunStatus(poolCtx, t, s, runID, drevtypes.StatusSuccess, 10*time.Second)
	if finalRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected recovered run status success, got %s", finalRun.Status)
	}

	// 6. Verify database state:
	//    - Both jobs registered and completed
	//    - Outbox entry deleted
	jobs, err := s.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("GetRunJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs registered for recovered run, got %d", len(jobs))
	}
	for _, j := range jobs {
		if j.Status != drevtypes.StatusSuccess {
			t.Errorf("recovered job %s status %s, want success", j.JobName, j.Status)
		}
	}

	remainingOutbox, _ := s.GetPendingOutbox(ctx, 10)
	if len(remainingOutbox) != 0 {
		t.Errorf("expected outbox to be cleared after recovery and claim, got %d", len(remainingOutbox))
	}

	executed := mr.getExecutedJobs()
	if len(executed) != 2 || executed[0] != "step-1" || executed[1] != "step-2" {
		t.Errorf("expected execution order [step-1, step-2], got %v", executed)
	}
}

// ── Test D: Dynamic Execution Failure Paths ─────────────────────────────────

func TestE2E_DynamicExecution_FailurePaths(t *testing.T) {
	testCases := []struct {
		name              string
		files             map[string]string
		expectedLogSubstr string
	}{
		{
			name: "missing_repo_config",
			files: map[string]string{
				"src/main.go": "package main\n",
			},
			expectedLogSubstr: "resolving repository configuration",
		},
		{
			name: "malformed_yaml_config",
			files: map[string]string{
				".drev.yml": `name: [malformed
  jobs: {invalid: yaml:::
`,
			},
			expectedLogSubstr: "resolving repository configuration",
		},
		{
			name: "invalid_dag_cycle",
			files: map[string]string{
				".drev.yml": `name: cyclic-pipeline
jobs:
  - name: job-alpha
    image: alpine:latest
    depends_on: [job-beta]
    steps:
      - name: step-a
        run: echo a
  - name: job-beta
    image: alpine:latest
    depends_on: [job-alpha]
    steps:
      - name: step-b
        run: echo b
`,
			},
			expectedLogSubstr: "circular dependency detected",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repoDir, headSHA := createTestGitRepo(t, tc.files)

			h := setupE2EHarness(t, nil)

			// Deliver push webhook
			payload := webhook.PushPayload{
				Ref: "refs/heads/main",
			}
			payload.Repository.FullName = "acme/fail-" + tc.name
			payload.Repository.CloneURL = repoDir
			payload.HeadCommit.ID = headSHA
			payload.Pusher.Name = "tester"
			bodyBytes, _ := json.Marshal(payload)

			deliveryID := "del-fail-" + tc.name
			req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Hub-Signature-256", signWebhookBody(h.secret, bodyBytes))
			req.Header.Set("X-GitHub-Event", "push")
			req.Header.Set("X-GitHub-Delivery", deliveryID)

			rr := httptest.NewRecorder()
			h.webhook.Handle(rr, req)

			// Webhook itself must accept valid event with 202
			if rr.Code != http.StatusAccepted {
				t.Fatalf("expected HTTP 202 Accepted at ingestion, got %d: %s", rr.Code, rr.Body.String())
			}

			var resp map[string]interface{}
			json.NewDecoder(rr.Body).Decode(&resp)
			runID := resp["run_id"].(string)

			ctx := context.Background()

			// Zero jobs before execution
			initialJobs, _ := h.store.GetRunJobs(ctx, runID)
			if len(initialJobs) != 0 {
				t.Fatalf("expected 0 jobs in DB before worker, got %d", len(initialJobs))
			}

			// Start worker pool
			poolCtx, poolCancel := context.WithCancel(context.Background())
			defer poolCancel()
			t.Cleanup(func() {
				poolCancel()
				h.pool.Stop()
			})
			h.pool.Start(poolCtx)

			// Wait for run to fail
			finalRun := waitForRunStatus(poolCtx, t, h.store, runID, drevtypes.StatusFailed, 10*time.Second)
			if finalRun.Status != drevtypes.StatusFailed {
				t.Fatalf("expected run status failed, got %s", finalRun.Status)
			}
			if finalRun.FinishedAt.IsZero() {
				t.Errorf("expected FinishedAt to be populated for failed run")
			}

			// Verify runner was NEVER invoked
			executed := h.runner.getExecutedJobs()
			if len(executed) != 0 {
				t.Errorf("mock runner should not have executed any jobs on preparation failure, executed: %v", executed)
			}

			// Verify no jobs remain registered in DB
			dbJobs, err := h.store.GetRunJobs(ctx, runID)
			if err != nil {
				t.Fatalf("GetRunJobs: %v", err)
			}
			if len(dbJobs) != 0 {
				t.Errorf("expected 0 jobs registered in DB on preparation failure, found %d", len(dbJobs))
			}

			// Verify outbox entry was deleted upon worker claim
			outbox, _ := h.store.GetPendingOutbox(ctx, 10)
			if len(outbox) != 0 {
				t.Errorf("expected outbox entry to be cleared after claim, found %d", len(outbox))
			}

			// Verify streamer log contains expected diagnostic
			logReader, err := h.streamer.Reader(runID)
			if err != nil {
				t.Fatalf("streamer.Reader: %v", err)
			}
			defer logReader.Close()
			logBytes, _ := io.ReadAll(logReader)
			logText := string(logBytes)
			if !strings.Contains(logText, tc.expectedLogSubstr) {
				t.Errorf("expected log to contain %q, got: %s", tc.expectedLogSubstr, logText)
			}
		})
	}
}

// ── Test E: Optional PostgreSQL End-to-End Verification ────────────────────

func TestE2E_PushWebhook_To_SuccessfulDynamicExecution_Postgres(t *testing.T) {
	connStr := os.Getenv("DREV_DB_URL")
	if connStr == "" {
		t.Skip("DREV_DB_URL not set — skipping optional PostgreSQL E2E test")
	}

	pg, err := store.OpenPostgres(connStr)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() { pg.Close() })

	pipelineYAML := `name: e2e-postgres-pipeline
jobs:
  - name: compile
    image: alpine:latest
    steps:
      - name: run-compile
        run: echo "compile"
  - name: test
    image: alpine:latest
    depends_on: [compile]
    steps:
      - name: run-test
        run: echo "test"
`
	repoDir, headSHA := createTestGitRepo(t, map[string]string{
		".drev.yml":  pipelineYAML,
		"src/app.go": "package app\n",
	})

	h := setupE2EHarness(t, pg)

	payload := webhook.PushPayload{
		Ref: "refs/heads/main",
	}
	payload.Repository.FullName = "acme/postgres-e2e"
	payload.Repository.CloneURL = repoDir
	payload.HeadCommit.ID = headSHA
	payload.Pusher.Name = "pg-tester"
	bodyBytes, _ := json.Marshal(payload)

	deliveryID := fmt.Sprintf("del-pg-e2e-%d", time.Now().UnixNano())
	req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(h.secret, bodyBytes))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.webhook.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	runID := resp["run_id"].(string)

	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()
	t.Cleanup(func() {
		poolCancel()
		h.pool.Stop()
	})
	h.pool.Start(poolCtx)

	finalRun := waitForRunStatus(poolCtx, t, pg, runID, drevtypes.StatusSuccess, 10*time.Second)
	if finalRun.Status != drevtypes.StatusSuccess {
		t.Fatalf("expected postgres run status success, got %s", finalRun.Status)
	}

	ctx := context.Background()
	jobs, err := pg.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("pg.GetRunJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs in postgres, got %d", len(jobs))
	}
}
