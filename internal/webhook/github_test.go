package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/pool"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/pkg/drevtypes"
)

const validPipelineYAML = `
name: test
jobs:
  - name: test
    image: alpine
    steps:
      - name: test
        run: echo "test"
`

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func setupTestHandler(t *testing.T, secret string, configDir string) *GitHubHandler {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}

	p := parser.NewParser()
	q := queue.New(10)
	stream := streamer.New(t.TempDir())

	t.Cleanup(func() {
		s.Close()
		// Drain queue and close any log writers to avoid Windows file lock issues
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

	return New(s, q, p, stream, secret, configDir)
}

func TestGitHubWebhook_MissingSignature(t *testing.T) {
	h := setupTestHandler(t, "secret", t.TempDir())

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader([]byte("{}")))
	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected %v, got %v", http.StatusBadRequest, rr.Code)
	}
}

func TestGitHubWebhook_InvalidSignature(t *testing.T) {
	h := setupTestHandler(t, "secret", t.TempDir())

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader([]byte("{}")))
	req.Header.Set("X-Hub-Signature-256", "sha256=invalid")
	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected %v, got %v", http.StatusUnauthorized, rr.Code)
	}
}

func TestGitHubWebhook_NonPushEvent(t *testing.T) {
	body := []byte("{}")
	h := setupTestHandler(t, "secret", t.TempDir())

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "ping")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected %v, got %v", http.StatusOK, rr.Code)
	}
	if rr.Body.String() != "event ignored" {
		t.Errorf("expected 'event ignored', got %v", rr.Body.String())
	}
}

func TestGitHubWebhook_MissingDeliveryID(t *testing.T) {
	h := setupTestHandler(t, "secret", t.TempDir())

	// Push event with missing delivery ID
	body := []byte(`{"ref":"refs/heads/main"}`)
	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %v", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "missing delivery id") {
		t.Errorf("expected error message 'missing delivery id', got: %s", rr.Body.String())
	}

	// Pull request event with missing delivery ID
	reqPR, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	reqPR.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	reqPR.Header.Set("X-GitHub-Event", "pull_request")
	rrPR := httptest.NewRecorder()
	h.Handle(rrPR, reqPR)

	if rrPR.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %v", rrPR.Code)
	}
	if !strings.Contains(rrPR.Body.String(), "missing delivery id") {
		t.Errorf("expected error message 'missing delivery id', got: %s", rrPR.Body.String())
	}
}

func TestGitHubWebhook_NoPipelineConfig(t *testing.T) {
	payload := PushPayload{}
	payload.Repository.FullName = "octocat/hello-world"
	payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = "1234567890abcdef1234567890abcdef12345678"
	body, _ := json.Marshal(payload)

	h := setupTestHandler(t, "secret", t.TempDir())

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-no-config-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected %v, got %v: %s", http.StatusAccepted, rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	runID, ok := resp["run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("expected run_id in response, got %v", resp)
	}

	ctx := context.Background()
	run, err := h.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if run.Status != drevtypes.StatusPending {
		t.Errorf("expected run status pending, got %v", run.Status)
	}

	jobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 pre-registered jobs for bootstrap run, got %d", len(jobs))
	}

	select {
	case job := <-h.queue.Drain():
		if len(job.Pipeline.Jobs) != 0 {
			t.Errorf("expected 0 jobs in enqueued pipeline, got %d", len(job.Pipeline.Jobs))
		}
		if job.Pipeline.Source.CommitSHA != "1234567890abcdef1234567890abcdef12345678" {
			t.Errorf("expected job source CommitSHA, got %s", job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.Ref != "main" {
			t.Errorf("expected job source Ref main, got %s", job.Pipeline.Source.Ref)
		}
	default:
		t.Fatal("expected job to be enqueued")
	}
}

func TestGitHubWebhook_ValidPush(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)

	payload := PushPayload{}
	payload.Repository.FullName = "octocat/hello-world"
	payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = "1234567890abcdef1234567890abcdef12345678"
	payload.HeadCommit.Message = "Initial commit"
	payload.Pusher.Name = "octocat"
	body, _ := json.Marshal(payload)

	h := setupTestHandler(t, "secret", configDir)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-valid-push-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected %v, got %v: %s", http.StatusAccepted, rr.Code, rr.Body.String())
	}

	var resp map[string]string
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp["run_id"] == "" || resp["repo"] != "octocat/hello-world" {
		t.Errorf("invalid response body: %v", resp)
	}
	if resp["commit"] != "1234567" {
		t.Errorf("expected commit 1234567, got %s", resp["commit"])
	}
	if resp["branch"] != "main" {
		t.Errorf("expected branch main, got %s", resp["branch"])
	}

	ctx := context.Background()
	run, err := h.store.GetRun(ctx, resp["run_id"])
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if run.CommitSHA != "1234567890abcdef1234567890abcdef12345678" {
		t.Errorf("expected run commit SHA, got %s", run.CommitSHA)
	}
	if run.Status != drevtypes.StatusPending {
		t.Errorf("expected run status pending, got %s", run.Status)
	}

	jobs, err := h.store.GetRunJobs(ctx, resp["run_id"])
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs for bootstrap run, got %d", len(jobs))
	}
}

func TestGitHubWebhook_QueueFull_MarksRunAndJobsFailed(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "queue-full-repo")
	os.MkdirAll(repoDir, 0755)

	payload := PushPayload{}
	payload.Repository.FullName = "octocat/queue-full-repo"
	payload.Repository.CloneURL = "https://github.com/octocat/queue-full-repo.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = "1234567890abcdef1234567890abcdef12345678"
	payload.Pusher.Name = "octocat"
	body, _ := json.Marshal(payload)

	h := setupTestHandler(t, "secret", configDir)

	// Fill the queue to capacity (size 10 in setupTestHandler)
	for i := 0; i < 10; i++ {
		if err := h.queue.Enqueue(&queue.Job{
			RunID:      fmt.Sprintf("fill-%d", i),
			Pipeline:   &drevtypes.Pipeline{Name: "fill"},
			LogWriter:  nil,
			EnqueuedAt: time.Now(),
		}); err != nil {
			t.Fatalf("filling queue at %d: %v", i, err)
		}
	}

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-queue-full-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable, got %v", rr.Code)
	}

	// Verify in store that the run is StatusFailed
	runs, err := h.store.ListRuns(context.Background(), 50)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	var targetRun *drevtypes.Run
	for _, r := range runs {
		if r.PipelineID == "octocat/queue-full-repo" {
			targetRun = r
			break
		}
	}
	if targetRun == nil {
		t.Fatal("expected run record in store, got none")
	}
	if targetRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected run status %q, got %q", drevtypes.StatusFailed, targetRun.Status)
	}
	if targetRun.FinishedAt.IsZero() {
		t.Errorf("expected finished_at to be populated for failed run")
	}

	// In bootstrap mode, zero jobs are pre-registered
	jobs, err := h.store.GetRunJobs(context.Background(), targetRun.ID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs for bootstrap run, got %d", len(jobs))
	}

	// Verify rejection reason is recorded in streamer log
	r, err := h.streamer.Reader(targetRun.ID)
	if err != nil {
		t.Fatalf("reading run log: %v", err)
	}
	defer r.Close()
	logContent, _ := io.ReadAll(r)
	if !strings.Contains(string(logContent), "[drev] pipeline rejected: queue is full") {
		t.Errorf("expected log to contain rejection reason, got: %s", string(logContent))
	}
}

func makePRPayload(action string, number int, repo, cloneURL, headRef, headSHA, title, userLogin string, isFork bool, headRepoName string) PullRequestPayload {
	if headRepoName == "" {
		headRepoName = repo
	}
	p := PullRequestPayload{
		Action: action,
		Number: number,
	}
	p.PullRequest.Number = number
	p.PullRequest.Title = title
	p.PullRequest.User.Login = userLogin
	p.PullRequest.Head.Ref = headRef
	p.PullRequest.Head.SHA = headSHA
	p.PullRequest.Head.Repo.FullName = headRepoName
	p.PullRequest.Head.Repo.CloneURL = cloneURL
	p.PullRequest.Head.Repo.Fork = isFork
	p.PullRequest.Base.Ref = "main"
	p.PullRequest.Base.Repo.FullName = repo
	p.PullRequest.Base.Repo.CloneURL = cloneURL
	p.Repository.FullName = repo
	p.Repository.CloneURL = cloneURL
	p.Repository.DefaultBranch = "main"
	p.Sender.Login = userLogin
	return p
}

func TestGitHubWebhook_PullRequest_Opened(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)

	pipelineYAML := `
name: pr-pipeline
jobs:
  - name: build
    image: golang:1.24
    steps:
      - name: test
        run: go test ./...
`
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(pipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	payload := makePRPayload(
		"opened", 42,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"feature-login", "a1b2c3d4e5f678901234567890abcdef12345678",
		"Add new login page", "octocat-dev",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-pr-opened-001")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response json: %v", err)
	}

	runID, ok := resp["run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("expected run_id in response, got %v", resp)
	}
	if resp["repo"] != "octocat/hello-world" {
		t.Errorf("expected repo octocat/hello-world, got %v", resp["repo"])
	}
	if resp["branch"] != "feature-login" {
		t.Errorf("expected branch feature-login, got %v", resp["branch"])
	}
	if resp["commit"] != "a1b2c3d" {
		t.Errorf("expected commit a1b2c3d, got %v", resp["commit"])
	}
	if resp["triggered_by"] != "octocat-dev" {
		t.Errorf("expected triggered_by octocat-dev, got %v", resp["triggered_by"])
	}
	if resp["event"] != "pull_request" {
		t.Errorf("expected event pull_request, got %v", resp["event"])
	}
	if resp["delivery_id"] != "delivery-pr-opened-001" {
		t.Errorf("expected delivery_id delivery-pr-opened-001, got %v", resp["delivery_id"])
	}
	if prNum, ok := resp["pr_number"].(float64); !ok || int(prNum) != 42 {
		t.Errorf("expected pr_number 42, got %v", resp["pr_number"])
	}

	// Verify run metadata in store
	ctx := context.Background()
	run, err := h.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("h.store.GetRun: %v", err)
	}
	if run.Status != drevtypes.StatusPending {
		t.Errorf("expected run status pending, got %v", run.Status)
	}
	if run.CommitSHA != "a1b2c3d4e5f678901234567890abcdef12345678" {
		t.Errorf("expected CommitSHA, got %v", run.CommitSHA)
	}
	if run.CommitMsg != "Add new login page" {
		t.Errorf("expected CommitMsg 'Add new login page', got %v", run.CommitMsg)
	}
	if run.Branch != "feature-login" {
		t.Errorf("expected Branch feature-login, got %v", run.Branch)
	}
	if run.TriggeredBy != "octocat-dev" {
		t.Errorf("expected TriggeredBy octocat-dev, got %v", run.TriggeredBy)
	}

	// Verify delivery recorded
	del, err := h.store.GetDelivery(ctx, "delivery-pr-opened-001")
	if err != nil {
		t.Fatalf("h.store.GetDelivery: %v", err)
	}
	if del == nil || del.RunID != runID || del.PRNumber != 42 {
		t.Fatalf("unexpected delivery record: %+v", del)
	}

	// Verify enqueued job has Source PR metadata
	select {
	case job := <-h.queue.Drain():
		if job.Pipeline.Source.CommitSHA != "a1b2c3d4e5f678901234567890abcdef12345678" {
			t.Errorf("expected job source CommitSHA, got %s", job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.Ref != "feature-login" {
			t.Errorf("expected job source Ref feature-login, got %s", job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.PRNumber != 42 {
			t.Errorf("expected job source PRNumber 42, got %d", job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected job to be enqueued")
	}

	// Verify outbox record contains Source PR metadata
	outboxItems, err := h.store.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("h.store.GetPendingOutbox: %v", err)
	}
	if len(outboxItems) != 1 {
		t.Fatalf("expected 1 outbox item, got %d", len(outboxItems))
	}
	var outboxPipeline drevtypes.Pipeline
	if err := json.Unmarshal([]byte(outboxItems[0].PipelineSpec), &outboxPipeline); err != nil {
		t.Fatalf("unmarshaling outbox pipeline spec: %v", err)
	}
	if outboxPipeline.Source.CommitSHA != "a1b2c3d4e5f678901234567890abcdef12345678" {
		t.Errorf("expected outbox source CommitSHA, got %s", outboxPipeline.Source.CommitSHA)
	}
	if outboxPipeline.Source.Ref != "feature-login" {
		t.Errorf("expected outbox source Ref feature-login, got %s", outboxPipeline.Source.Ref)
	}
	if outboxPipeline.Source.PRNumber != 42 {
		t.Errorf("expected outbox source PRNumber 42, got %d", outboxPipeline.Source.PRNumber)
	}
}

func TestGitHubWebhook_PullRequest_Synchronize(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	payload := makePRPayload(
		"synchronize", 42,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"feature-login", "b1b2c3d4e5f678901234567890abcdef12345678",
		"Add new login page - sync", "octocat-dev",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-pr-sync-001")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	// Verify enqueued job has updated Source metadata for synchronize
	select {
	case job := <-h.queue.Drain():
		if job.Pipeline.Source.CommitSHA != "b1b2c3d4e5f678901234567890abcdef12345678" {
			t.Errorf("expected job source CommitSHA b1b2..., got %s", job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.Ref != "feature-login" {
			t.Errorf("expected job source Ref feature-login, got %s", job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.PRNumber != 42 {
			t.Errorf("expected job source PRNumber 42, got %d", job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected synchronize job to be enqueued")
	}
}

func TestGitHubWebhook_PullRequest_Reopened(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	payload := makePRPayload(
		"reopened", 42,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"feature-login", "c1c2c3c4c5c678901234567890abcdef12345678",
		"Add new login page - reopen", "octocat-dev",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-pr-reopened-001")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	// Verify enqueued job has updated Source metadata for reopen
	select {
	case job := <-h.queue.Drain():
		if job.Pipeline.Source.CommitSHA != "c1c2c3c4c5c678901234567890abcdef12345678" {
			t.Errorf("expected job source CommitSHA c1c2..., got %s", job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.Ref != "feature-login" {
			t.Errorf("expected job source Ref feature-login, got %s", job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.PRNumber != 42 {
			t.Errorf("expected job source PRNumber 42, got %d", job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected reopen job to be enqueued")
	}
}

func TestGitHubWebhook_PullRequest_PropagatesSourceRevisionMetadata(t *testing.T) {
	actions := []string{"opened", "synchronize", "reopened"}

	for _, act := range actions {
		t.Run(act, func(t *testing.T) {
			configDir := t.TempDir()
			repoDir := filepath.Join(configDir, "octocat", "hello-world")
			os.MkdirAll(repoDir, 0755)
			os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

			h := setupTestHandler(t, "secret", configDir)

			headSHA := "e1e2e3e4e5e678901234567890abcdef12345678"
			headRef := "refs/heads/feature-branch"
			cloneURL := "https://github.com/octocat/hello-world.git"
			prNum := 77

			payload := makePRPayload(
				act, prNum,
				"octocat/hello-world", cloneURL,
				headRef, headSHA,
				"PR for "+act, "alice",
				false, "octocat/hello-world",
			)
			body, _ := json.Marshal(payload)

			deliveryID := fmt.Sprintf("del-prop-%s", act)
			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-GitHub-Delivery", deliveryID)

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			if rr.Code != http.StatusAccepted {
				t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
			}

			// Verify in-memory queue job source fields
			select {
			case job := <-h.queue.Drain():
				if job.Pipeline.Source.URL != cloneURL {
					t.Errorf("expected URL %s, got %s", cloneURL, job.Pipeline.Source.URL)
				}
				if job.Pipeline.Source.Ref != headRef {
					t.Errorf("expected Ref %s, got %s", headRef, job.Pipeline.Source.Ref)
				}
				if job.Pipeline.Source.CommitSHA != headSHA {
					t.Errorf("expected CommitSHA %s, got %s", headSHA, job.Pipeline.Source.CommitSHA)
				}
				if job.Pipeline.Source.PRNumber != prNum {
					t.Errorf("expected PRNumber %d, got %d", prNum, job.Pipeline.Source.PRNumber)
				}
			default:
				t.Fatal("expected job to be in queue")
			}

			// Verify durable outbox source fields
			ctx := context.Background()
			outbox, err := h.store.GetPendingOutbox(ctx, 10)
			if err != nil {
				t.Fatalf("GetPendingOutbox: %v", err)
			}
			if len(outbox) != 1 {
				t.Fatalf("expected 1 outbox item, got %d", len(outbox))
			}

			var parsed drevtypes.Pipeline
			if err := json.Unmarshal([]byte(outbox[0].PipelineSpec), &parsed); err != nil {
				t.Fatalf("unmarshaling outbox spec: %v", err)
			}
			if parsed.Source.URL != cloneURL {
				t.Errorf("outbox Source.URL mismatch: expected %s, got %s", cloneURL, parsed.Source.URL)
			}
			if parsed.Source.Ref != headRef {
				t.Errorf("outbox Source.Ref mismatch: expected %s, got %s", headRef, parsed.Source.Ref)
			}
			if parsed.Source.CommitSHA != headSHA {
				t.Errorf("outbox Source.CommitSHA mismatch: expected %s, got %s", headSHA, parsed.Source.CommitSHA)
			}
			if parsed.Source.PRNumber != prNum {
				t.Errorf("outbox Source.PRNumber mismatch: expected %d, got %d", prNum, parsed.Source.PRNumber)
			}
		})
	}
}

func TestGitHubWebhook_PullRequest_MalformedHeadSHA(t *testing.T) {
	cases := []struct {
		name    string
		headSHA string
		headRef string
		prNum   int
	}{
		{
			name:    "empty head SHA",
			headSHA: "",
			headRef: "feature-branch",
			prNum:   100,
		},
		{
			name:    "short SHA (7 chars)",
			headSHA: "a1b2c3d",
			headRef: "feature-branch",
			prNum:   100,
		},
		{
			name:    "short SHA (16 chars)",
			headSHA: "1234567890abcdef",
			headRef: "feature-branch",
			prNum:   100,
		},
		{
			name:    "non-hex letters in 40-char string",
			headSHA: "g1b2c3d4e5f678901234567890abcdef12345678",
			headRef: "feature-branch",
			prNum:   100,
		},
		{
			name:    "option-like SHA starting with dash",
			headSHA: "-1b2c3d4e5f678901234567890abcdef12345678",
			headRef: "feature-branch",
			prNum:   100,
		},
		{
			name:    "option-like Ref starting with dash",
			headSHA: "a1b2c3d4e5f678901234567890abcdef12345678",
			headRef: "--upload-pack=evil",
			prNum:   100,
		},
		{
			name:    "zero PR number",
			headSHA: "a1b2c3d4e5f678901234567890abcdef12345678",
			headRef: "feature-branch",
			prNum:   0,
		},
		{
			name:    "negative PR number",
			headSHA: "a1b2c3d4e5f678901234567890abcdef12345678",
			headRef: "feature-branch",
			prNum:   -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configDir := t.TempDir()
			repoDir := filepath.Join(configDir, "octocat", "hello-world")
			os.MkdirAll(repoDir, 0755)
			os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

			h := setupTestHandler(t, "secret", configDir)

			payload := makePRPayload(
				"opened", tc.prNum,
				"octocat/hello-world", "https://github.com/octocat/hello-world.git",
				tc.headRef, tc.headSHA,
				"Test PR", "octocat-dev",
				false, "octocat/hello-world",
			)
			body, _ := json.Marshal(payload)

			deliveryID := "del-malformed-" + strings.ReplaceAll(tc.name, " ", "-")
			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-GitHub-Delivery", deliveryID)

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request, got %v: %s", rr.Code, rr.Body.String())
			}

			// Verify nothing was persisted or queued
			ctx := context.Background()
			del, err := h.store.GetDelivery(ctx, deliveryID)
			if err != nil {
				t.Fatalf("GetDelivery error: %v", err)
			}
			if del != nil {
				t.Errorf("expected no delivery record created for malformed request, found: %+v", del)
			}

			outbox, err := h.store.GetPendingOutbox(ctx, 10)
			if err != nil {
				t.Fatalf("GetPendingOutbox error: %v", err)
			}
			if len(outbox) != 0 {
				t.Errorf("expected 0 outbox entries, found: %d", len(outbox))
			}

			if h.queue.Depth() != 0 {
				t.Errorf("expected 0 queued jobs, found: %d", h.queue.Depth())
			}
		})
	}
}

func TestGitHubWebhook_PullRequest_UnsupportedAction(t *testing.T) {
	unsupportedActions := []string{"closed", "labeled", "unlabeled", "assigned", "unassigned", "edited", "review_requested"}

	for _, act := range unsupportedActions {
		t.Run(act, func(t *testing.T) {
			h := setupTestHandler(t, "secret", t.TempDir())

			payload := makePRPayload(
				act, 10,
				"octocat/hello-world", "https://github.com/octocat/hello-world.git",
				"main", "1234567890abcdef1234567890abcdef12345678",
				"PR title", "octocat",
				false, "octocat/hello-world",
			)
			body, _ := json.Marshal(payload)

			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-GitHub-Delivery", fmt.Sprintf("delivery-unsupported-%s", act))

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("action %s: expected 200 OK, got %v", act, rr.Code)
			}
			if rr.Body.String() != "event ignored" {
				t.Errorf("action %s: expected 'event ignored', got %q", act, rr.Body.String())
			}

			// Verify no runs or deliveries were created
			runs, _ := h.store.ListRuns(context.Background(), 10)
			if len(runs) != 0 {
				t.Errorf("action %s: expected 0 runs in store, got %d", act, len(runs))
			}
		})
	}
}

func TestGitHubWebhook_PullRequest_ForkDenied(t *testing.T) {
	t.Run("head_repo_marked_fork", func(t *testing.T) {
		configDir := t.TempDir()
		repoDir := filepath.Join(configDir, "octocat", "hello-world")
		os.MkdirAll(repoDir, 0755)
		os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

		h := setupTestHandler(t, "secret", configDir)

		payload := makePRPayload(
			"opened", 100,
			"octocat/hello-world", "https://github.com/octocat/hello-world.git",
			"patch-1", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			"Untrusted fork contribution", "attacker",
			true, "octocat/hello-world", // Fork marked true
		)
		body, _ := json.Marshal(payload)

		req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
		req.Header.Set("X-GitHub-Event", "pull_request")
		req.Header.Set("X-GitHub-Delivery", "delivery-fork-001")

		rr := httptest.NewRecorder()
		h.Handle(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden, got %v: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "fork pull requests are not permitted by policy") {
			t.Errorf("expected fork policy rejection message, got: %s", rr.Body.String())
		}

		// Verify 0 runs and 0 deliveries created
		runs, _ := h.store.ListRuns(context.Background(), 10)
		if len(runs) != 0 {
			t.Errorf("expected 0 runs, got %d", len(runs))
		}
		del, _ := h.store.GetDelivery(context.Background(), "delivery-fork-001")
		if del != nil {
			t.Errorf("expected no delivery recorded for rejected fork PR")
		}
	})

	t.Run("head_repo_different_fullname", func(t *testing.T) {
		configDir := t.TempDir()
		repoDir := filepath.Join(configDir, "octocat", "hello-world")
		os.MkdirAll(repoDir, 0755)
		os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

		h := setupTestHandler(t, "secret", configDir)

		payload := makePRPayload(
			"opened", 101,
			"octocat/hello-world", "https://github.com/octocat/hello-world.git",
			"patch-2", "beefdeadbeefdeadbeefdeadbeefdeadbeefdead",
			"External repo fork contribution", "external-user",
			false, "external-user/hello-world", // Different full_name
		)
		body, _ := json.Marshal(payload)

		req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
		req.Header.Set("X-GitHub-Event", "pull_request")
		req.Header.Set("X-GitHub-Delivery", "delivery-fork-002")

		rr := httptest.NewRecorder()
		h.Handle(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden, got %v: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "fork pull requests are not permitted by policy") {
			t.Errorf("expected fork policy rejection message, got: %s", rr.Body.String())
		}

		runs, _ := h.store.ListRuns(context.Background(), 10)
		if len(runs) != 0 {
			t.Errorf("expected 0 runs, got %d", len(runs))
		}
	})
}

func TestGitHubWebhook_MalformedPayloads(t *testing.T) {
	tests := []struct {
		name       string
		event      string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid_json_push",
			event:      "push",
			body:       "{not-valid-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json_pr",
			event:      "pull_request",
			body:       "{\"action\": \"opened\", not json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "path_traversal_in_repo_name",
			event:      "push",
			body:       `{"ref":"refs/heads/main","repository":{"full_name":"../../etc/passwd","clone_url":"https://github.com/a/b.git"},"head_commit":{"id":"1234567890abcdef1234567890abcdef12345678"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "push_malformed_head_commit_sha",
			event:      "push",
			body:       `{"ref":"refs/heads/main","repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"head_commit":{"id":"abc1234"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "push_missing_head_commit_and_after",
			event:      "push",
			body:       `{"ref":"refs/heads/main","repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "push_missing_repo_fullname",
			event:      "push",
			body:       `{"ref":"refs/heads/main","repository":{"clone_url":"https://github.com/octocat/hello-world.git"},"head_commit":{"id":"1234567890abcdef1234567890abcdef12345678"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "push_missing_clone_url",
			event:      "push",
			body:       `{"ref":"refs/heads/main","repository":{"full_name":"octocat/hello-world"},"head_commit":{"id":"1234567890abcdef1234567890abcdef12345678"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "push_missing_ref",
			event:      "push",
			body:       `{"repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"head_commit":{"id":"1234567890abcdef1234567890abcdef12345678"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "pr_missing_head_sha",
			event:      "pull_request",
			body:       `{"action":"opened","number":1,"repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"pull_request":{"number":1,"head":{"ref":"main","repo":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"}}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "pr_missing_head_ref",
			event:      "pull_request",
			body:       `{"action":"opened","number":1,"repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"pull_request":{"number":1,"head":{"sha":"1234567890abcdef1234567890abcdef12345678","repo":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"}}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "pr_missing_number",
			event:      "pull_request",
			body:       `{"action":"opened","number":0,"repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"pull_request":{"number":0,"head":{"ref":"main","sha":"1234567890abcdef1234567890abcdef12345678","repo":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"}}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "pr_negative_number",
			event:      "pull_request",
			body:       `{"action":"opened","number":-5,"repository":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"},"pull_request":{"number":-5,"head":{"ref":"main","sha":"1234567890abcdef1234567890abcdef12345678","repo":{"full_name":"octocat/hello-world","clone_url":"https://github.com/octocat/hello-world.git"}}}}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := setupTestHandler(t, "secret", t.TempDir())
			body := []byte(tc.body)
			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", tc.event)
			req.Header.Set("X-GitHub-Delivery", "delivery-malformed-001")

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			if rr.Code != tc.wantStatus {
				t.Errorf("expected status %v, got %v: %s", tc.wantStatus, rr.Code, rr.Body.String())
			}

			// Verify 0 runs created
			runs, _ := h.store.ListRuns(context.Background(), 10)
			if len(runs) != 0 {
				t.Errorf("expected 0 runs, got %d", len(runs))
			}
		})
	}
}

func TestGitHubWebhook_DuplicateDelivery_Ignored(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	payload := makePRPayload(
		"opened", 55,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"PR for duplicate delivery test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)
	deliveryID := "delivery-duplicate-test-guid-123"

	// First request: should succeed and return 202 Accepted
	req1, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req1.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req1.Header.Set("X-GitHub-Event", "pull_request")
	req1.Header.Set("X-GitHub-Delivery", deliveryID)

	rr1 := httptest.NewRecorder()
	h.Handle(rr1, req1)

	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first delivery: expected 202 Accepted, got %v: %s", rr1.Code, rr1.Body.String())
	}

	var resp1 map[string]interface{}
	json.NewDecoder(rr1.Body).Decode(&resp1)
	firstRunID := resp1["run_id"].(string)

	// Second request with EXACT SAME delivery ID: should return 200 OK with duplicate status
	req2, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req2.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req2.Header.Set("X-GitHub-Event", "pull_request")
	req2.Header.Set("X-GitHub-Delivery", deliveryID)

	rr2 := httptest.NewRecorder()
	h.Handle(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("second delivery: expected 200 OK, got %v: %s", rr2.Code, rr2.Body.String())
	}

	var resp2 map[string]interface{}
	if err := json.NewDecoder(rr2.Body).Decode(&resp2); err != nil {
		t.Fatalf("decoding second response: %v", err)
	}
	if resp2["status"] != "duplicate" {
		t.Errorf("expected status 'duplicate', got %v", resp2["status"])
	}
	if resp2["delivery_id"] != deliveryID {
		t.Errorf("expected delivery_id %q, got %v", deliveryID, resp2["delivery_id"])
	}
	if resp2["run_id"] != firstRunID {
		t.Errorf("expected run_id %q, got %v", firstRunID, resp2["run_id"])
	}

	// Verify exactly ONE run in the store
	runs, err := h.store.ListRuns(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly 1 run, got %d", len(runs))
	}
	if runs[0].ID != firstRunID {
		t.Errorf("stored run ID %q != firstRunID %q", runs[0].ID, firstRunID)
	}
}

func TestGitHubWebhook_ConcurrentDuplicateDelivery(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	payload := makePRPayload(
		"opened", 77,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"PR for concurrent test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)
	deliveryID := "delivery-concurrent-guid-777"

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	type result struct {
		code int
		body string
	}
	results := make([]result, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-GitHub-Delivery", deliveryID)

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			results[idx] = result{code: rr.Code, body: rr.Body.String()}
		}(i)
	}

	wg.Wait()

	acceptedCount := 0
	duplicateCount := 0
	otherCount := 0

	for _, res := range results {
		switch res.code {
		case http.StatusAccepted:
			acceptedCount++
		case http.StatusOK:
			if strings.Contains(res.body, "duplicate") {
				duplicateCount++
			} else {
				otherCount++
			}
		default:
			otherCount++
			t.Errorf("unexpected status %d: %s", res.code, res.body)
		}
	}

	if acceptedCount != 1 {
		t.Errorf("expected exactly 1 accepted request, got %d", acceptedCount)
	}
	if duplicateCount != concurrency-1 {
		t.Errorf("expected %d duplicate responses, got %d", concurrency-1, duplicateCount)
	}
	if otherCount != 0 {
		t.Errorf("expected 0 other responses, got %d", otherCount)
	}

	// Verify store has exactly 1 run
	runs, err := h.store.ListRuns(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly 1 run in store, got %d", len(runs))
	}

	// Verify store has exactly 1 delivery record
	del, err := h.store.GetDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if del == nil {
		t.Fatal("expected delivery record, got nil")
	}
	if del.RunID != runs[0].ID {
		t.Errorf("delivery.RunID %q != run.ID %q", del.RunID, runs[0].ID)
	}
}

func TestGitHubWebhook_PushWithAfterFallback(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	// Payload with empty head_commit.id but valid after
	payload := PushPayload{
		Ref:   "refs/heads/feature-x",
		After: "a1b2c3d4e5f678901234567890abcdef12345678",
	}
	payload.Repository.FullName = "octocat/hello-world"
	payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
	payload.Sender.Login = "pusher-user"
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-push-after-001")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	runID := resp["run_id"].(string)

	run, _ := h.store.GetRun(context.Background(), runID)
	if run.CommitSHA != "a1b2c3d4e5f678901234567890abcdef12345678" {
		t.Errorf("expected CommitSHA from after, got %q", run.CommitSHA)
	}
	if run.Branch != "feature-x" {
		t.Errorf("expected Branch feature-x, got %q", run.Branch)
	}
	if run.TriggeredBy != "pusher-user" {
		t.Errorf("expected TriggeredBy pusher-user, got %q", run.TriggeredBy)
	}
}

func TestGitHubWebhook_InconsistentDeliveryState_Returns500(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	// Pre-insert an orphaned delivery record pointing to non-existent run
	deliveryID := "del-orphan-guid-999"
	orphanDelivery := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   12,
		RunID:      "non-existent-run-id",
		CreatedAt:  time.Now().UTC(),
	}
	if err := h.store.RecordDelivery(context.Background(), orphanDelivery); err != nil {
		t.Fatalf("setup RecordDelivery failed: %v", err)
	}

	payload := makePRPayload(
		"opened", 12,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Orphan delivery test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	// Must NOT return 200 duplicate; must return 500 error!
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error for missing run, got %v: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "inconsistent webhook delivery state") {
		t.Errorf("expected error message about inconsistent state, got: %s", rr.Body.String())
	}
}

func TestGitHubWebhook_QueueFull_ReleasesDeliveryForRetry(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	h := setupTestHandler(t, "secret", configDir)

	// Fill queue to capacity (10)
	for i := 0; i < 10; i++ {
		_ = h.queue.Enqueue(&queue.Job{
			RunID:      fmt.Sprintf("fill-%d", i),
			Pipeline:   &drevtypes.Pipeline{Name: "fill"},
			EnqueuedAt: time.Now(),
		})
	}

	deliveryID := "del-queue-retry-001"
	payload := makePRPayload(
		"opened", 13,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Queue retry test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	// First attempt: should fail with 503
	req1, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req1.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req1.Header.Set("X-GitHub-Event", "pull_request")
	req1.Header.Set("X-GitHub-Delivery", deliveryID)

	rr1 := httptest.NewRecorder()
	h.Handle(rr1, req1)

	if rr1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %v", rr1.Code)
	}

	// Verify delivery claim was released
	del1, _ := h.store.GetDelivery(context.Background(), deliveryID)
	if del1 != nil {
		t.Errorf("expected delivery claim to be released on queue full, got: %+v", del1)
	}

	// Verify run was finalized as StatusFailed
	runs, _ := h.store.ListRuns(context.Background(), 10)
	if len(runs) != 1 || runs[0].Status != drevtypes.StatusFailed {
		t.Fatalf("expected 1 failed run in store, got %+v", runs)
	}

	// Drain the queue to simulate worker capacity recovery
	for {
		select {
		case <-h.queue.Drain():
		default:
			goto drained
		}
	}
drained:

	// Second attempt (GitHub retry with SAME delivery ID): must succeed and return 202 Accepted!
	req2, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req2.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req2.Header.Set("X-GitHub-Event", "pull_request")
	req2.Header.Set("X-GitHub-Delivery", deliveryID)

	rr2 := httptest.NewRecorder()
	h.Handle(rr2, req2)

	if rr2.Code != http.StatusAccepted {
		t.Fatalf("retry after queue drained: expected 202 Accepted, got %v: %s", rr2.Code, rr2.Body.String())
	}

	// Now delivery IS claimed
	del2, _ := h.store.GetDelivery(context.Background(), deliveryID)
	if del2 == nil {
		t.Fatalf("expected delivery claim to be recorded on successful enqueue, got nil")
	}

	// Third attempt (duplicate after successful 202): must return 200 duplicate!
	req3, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req3.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req3.Header.Set("X-GitHub-Event", "pull_request")
	req3.Header.Set("X-GitHub-Delivery", deliveryID)

	rr3 := httptest.NewRecorder()
	h.Handle(rr3, req3)

	if rr3.Code != http.StatusOK {
		t.Fatalf("duplicate after success: expected 200 OK, got %v", rr3.Code)
	}
}

func TestGitHubWebhook_StreamerFailure_ReleasesDeliveryForRetry(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	// Create streamer with an un-creatable directory (file path where dir expected)
	invalidLogPath := filepath.Join(t.TempDir(), "not-a-dir")
	os.WriteFile(invalidLogPath, []byte("file"), 0644)
	stream := streamer.New(filepath.Join(invalidLogPath, "sub"))

	h := New(s, queue.New(10), parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-streamer-fail-001"
	payload := makePRPayload(
		"opened", 14,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Streamer fail test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on streamer failure, got %v", rr.Code)
	}

	// Delivery claim must be released so GitHub retry can succeed
	del, _ := s.GetDelivery(context.Background(), deliveryID)
	if del != nil {
		t.Errorf("expected delivery claim to be released on streamer error, got: %+v", del)
	}

	// Run must be marked StatusFailed
	runs, _ := s.ListRuns(context.Background(), 10)
	if len(runs) != 1 || runs[0].Status != drevtypes.StatusFailed {
		t.Errorf("expected 1 failed run, got: %+v", runs)
	}
}

func TestGitHubWebhook_DaemonRecovery_RetriesInterruptedDelivery(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	stream := streamer.New(t.TempDir())
	q := queue.New(10)
	h := New(s, q, parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-interrupted-001"
	payload := makePRPayload(
		"opened", 15,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Crash recovery test", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	// Simulate worker claimed run (status running, outbox cleared),
	// but process crashes mid-execution
	crashedRunID := "crashed-run-1"
	crashedDelivery := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  "pull_request",
		Repo:       "octocat/hello-world",
		PRNumber:   15,
		RunID:      crashedRunID,
		CreatedAt:  time.Now().UTC(),
	}
	crashedRun := &drevtypes.Run{
		ID:          crashedRunID,
		PipelineID:  "test",
		Status:      drevtypes.StatusPending,
		CommitSHA:   "1234567890abcdef1234567890abcdef12345678",
		TriggeredBy: "octocat",
	}
	crashedJobs := []*drevtypes.RunJob{
		{ID: "crashed-job-1", RunID: crashedRunID, JobName: "test", Status: drevtypes.StatusPending},
	}
	if err := s.CreateRunWithDelivery(context.Background(), crashedDelivery, crashedRun, crashedJobs, "{}"); err != nil {
		t.Fatalf("simulating crash state: %v", err)
	}
	if err := s.ClaimRunForExecution(context.Background(), crashedRunID); err != nil {
		t.Fatalf("simulating run claim: %v", err)
	}

	// Daemon restarts: ResetGhostRuns executes upon startup
	interrupted, err := s.ResetGhostRuns(context.Background())
	if err != nil {
		t.Fatalf("ResetGhostRuns failed: %v", err)
	}
	if len(interrupted) != 1 || interrupted[0] != crashedRunID {
		t.Fatalf("expected crashedRunID to be reset, got: %v", interrupted)
	}

	// Verify crashed run is now failed
	rCheck, _ := s.GetRun(context.Background(), crashedRunID)
	if rCheck.Status != drevtypes.StatusFailed || rCheck.FinishedAt.IsZero() {
		t.Fatalf("expected crashed run to be failed, got: %+v", rCheck)
	}

	// Verify delivery claim was released by daemon recovery
	dCheck, _ := s.GetDelivery(context.Background(), deliveryID)
	if dCheck != nil {
		t.Fatalf("expected delivery claim to be released, got: %+v", dCheck)
	}

	// GitHub automatic redelivery arrives with the SAME deliveryID:
	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	// Must succeed with 202 Accepted!
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on redelivery after daemon recovery, got %v: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	newRunID := resp["run_id"].(string)
	if newRunID == crashedRunID {
		t.Errorf("expected new run ID on redelivery, got same crashed run ID")
	}

	// Verify both runs exist in the database:
	// 1. The original failed run remains in history
	oldRun, _ := s.GetRun(context.Background(), crashedRunID)
	if oldRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected original run to remain failed in history")
	}
	// 2. The new run exists in pending status
	newRun, _ := s.GetRun(context.Background(), newRunID)
	if newRun.Status != drevtypes.StatusPending {
		t.Errorf("expected new run to be pending, got %v", newRun.Status)
	}

	// Subsequent duplicate request returns 200 duplicate pointing to newRunID
	reqDup, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	reqDup.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	reqDup.Header.Set("X-GitHub-Event", "pull_request")
	reqDup.Header.Set("X-GitHub-Delivery", deliveryID)

	rrDup := httptest.NewRecorder()
	h.Handle(rrDup, reqDup)

	if rrDup.Code != http.StatusOK {
		t.Fatalf("expected 200 OK duplicate, got %v", rrDup.Code)
	}
	var dupResp map[string]interface{}
	json.NewDecoder(rrDup.Body).Decode(&dupResp)
	if dupResp["run_id"] != newRunID {
		t.Errorf("expected duplicate response to point to %s, got %v", newRunID, dupResp["run_id"])
	}
}

func TestGitHubWebhook_AtomicFailureCleanup_QueueRejection(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "atomic_q_fail.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	stream := streamer.New(t.TempDir())
	// Queue size 1, pre-filled to trigger rejection
	q := queue.New(1)
	_ = q.Enqueue(&queue.Job{RunID: "filler", Pipeline: &drevtypes.Pipeline{Name: "filler"}})

	h := New(s, q, parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-atomic-q-001"
	payload := makePRPayload(
		"opened", 20,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Queue full atomic fail", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %v", rr.Code)
	}

	// 1. Delivery claim must be deleted to allow redelivery
	del, _ := s.GetDelivery(context.Background(), deliveryID)
	if del != nil {
		t.Errorf("delivery claim must be deleted on queue rejection, got: %+v", del)
	}

	// 2. Outbox entry must be deleted
	outbox, _ := s.GetPendingOutbox(context.Background(), 10)
	if len(outbox) != 0 {
		t.Errorf("outbox entry must be deleted on queue rejection, got: %d items", len(outbox))
	}

	// 3. Run must be marked StatusFailed, with zero pre-registered jobs
	runs, _ := s.ListRuns(context.Background(), 10)
	if len(runs) != 1 || runs[0].Status != drevtypes.StatusFailed {
		t.Fatalf("expected 1 failed run, got: %+v", runs)
	}
	jobs, _ := s.GetRunJobs(context.Background(), runs[0].ID)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs for bootstrap run, got: %d", len(jobs))
	}
}

func TestGitHubWebhook_AtomicFailureCleanup_StreamerFailure(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "atomic_streamer_fail.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	// Uncreatable directory to cause streamer failure
	invalidDir := filepath.Join(t.TempDir(), "not-a-dir")
	os.WriteFile(invalidDir, []byte("file"), 0644)
	stream := streamer.New(filepath.Join(invalidDir, "sub"))

	h := New(s, queue.New(10), parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-atomic-streamer-001"
	payload := makePRPayload(
		"opened", 21,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Streamer fail atomic", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on streamer failure, got %v", rr.Code)
	}

	// 1. Delivery claim must be deleted
	del, _ := s.GetDelivery(context.Background(), deliveryID)
	if del != nil {
		t.Errorf("delivery claim must be deleted on streamer error, got: %+v", del)
	}

	// 2. Outbox entry must be deleted
	outbox, _ := s.GetPendingOutbox(context.Background(), 10)
	if len(outbox) != 0 {
		t.Errorf("outbox entry must be deleted on streamer error, got: %d items", len(outbox))
	}

	// 3. Run and jobs must be StatusFailed
	runs, _ := s.ListRuns(context.Background(), 10)
	if len(runs) != 1 || runs[0].Status != drevtypes.StatusFailed {
		t.Fatalf("expected 1 failed run, got: %+v", runs)
	}
}

type failStoreWrapper struct {
	store.Store
	failRunWithDeliveryErr error
}

func (w *failStoreWrapper) FailRunWithDelivery(ctx context.Context, runID string, jobIDs []string, deliveryID string) error {
	if w.failRunWithDeliveryErr != nil {
		return w.failRunWithDeliveryErr
	}
	return w.Store.FailRunWithDelivery(ctx, runID, jobIDs, deliveryID)
}

func TestGitHubWebhook_QueueRejection_FailRunWithDeliveryError_Returns503(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "atomic_q_err.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	wrappedStore := &failStoreWrapper{
		Store:                  s,
		failRunWithDeliveryErr: errors.New("simulated database failure during cleanup"),
	}

	stream := streamer.New(t.TempDir())
	// Queue size 1, pre-filled to trigger rejection
	q := queue.New(1)
	_ = q.Enqueue(&queue.Job{RunID: "filler", Pipeline: &drevtypes.Pipeline{Name: "filler"}})

	h := New(wrappedStore, q, parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-atomic-q-err-001"
	payload := makePRPayload(
		"opened", 22,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Queue full cleanup error", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	// Even if FailRunWithDelivery fails, handler must preserve 503 response
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %v", rr.Code)
	}
}

func TestGitHubWebhook_StreamerFailure_FailRunWithDeliveryError_Returns500(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(validPipelineYAML), 0644)

	dbPath := filepath.Join(t.TempDir(), "atomic_streamer_err.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	wrappedStore := &failStoreWrapper{
		Store:                  s,
		failRunWithDeliveryErr: errors.New("simulated database failure during cleanup"),
	}

	// Uncreatable directory to cause streamer failure
	invalidDir := filepath.Join(t.TempDir(), "not-a-dir")
	os.WriteFile(invalidDir, []byte("file"), 0644)
	stream := streamer.New(filepath.Join(invalidDir, "sub"))

	h := New(wrappedStore, queue.New(10), parser.NewParser(), stream, "secret", configDir)

	deliveryID := "del-atomic-streamer-err-001"
	payload := makePRPayload(
		"opened", 23,
		"octocat/hello-world", "https://github.com/octocat/hello-world.git",
		"main", "1234567890abcdef1234567890abcdef12345678",
		"Streamer fail cleanup error", "octocat",
		false, "octocat/hello-world",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", deliveryID)

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	// Even if FailRunWithDelivery fails, handler must preserve 500 response
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on streamer failure, got %v", rr.Code)
	}
}

func TestGitHubWebhook_Push_BootstrapSpec_ZeroJobs(t *testing.T) {
	h := setupTestHandler(t, "secret", t.TempDir())

	headSHA := "1234567890abcdef1234567890abcdef12345678"
	payload := PushPayload{}
	payload.Repository.FullName = "org/my-bootstrap-repo"
	payload.Repository.CloneURL = "https://github.com/org/my-bootstrap-repo.git"
	payload.Ref = "refs/heads/feature-branch"
	payload.HeadCommit.ID = headSHA
	payload.HeadCommit.Message = "feat: bootstrap testing"
	payload.Pusher.Name = "committer"
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-push-bootstrap-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	runID, ok := resp["run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("expected run_id in response, got %v", resp)
	}
	if resp["repo"] != "org/my-bootstrap-repo" {
		t.Errorf("expected repo org/my-bootstrap-repo, got %v", resp["repo"])
	}
	if resp["branch"] != "feature-branch" {
		t.Errorf("expected branch feature-branch, got %v", resp["branch"])
	}
	if resp["commit"] != "1234567" {
		t.Errorf("expected commit short SHA 1234567, got %v", resp["commit"])
	}
	if resp["triggered_by"] != "committer" {
		t.Errorf("expected triggered_by committer, got %v", resp["triggered_by"])
	}

	// 1. Run in DB must be StatusPending with CommitSHA
	ctx := context.Background()
	run, err := h.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("getting run: %v", err)
	}
	if run.Status != drevtypes.StatusPending {
		t.Errorf("expected run status pending, got %v", run.Status)
	}
	if run.CommitSHA != headSHA {
		t.Errorf("expected run CommitSHA %s, got %s", headSHA, run.CommitSHA)
	}
	if run.PipelineID != "org/my-bootstrap-repo" {
		t.Errorf("expected run PipelineID org/my-bootstrap-repo, got %s", run.PipelineID)
	}

	// 2. Pre-registered jobs in DB must be exactly 0
	jobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 pre-registered jobs in DB, got %d", len(jobs))
	}

	// 3. Enqueued job must have 0 jobs and populated Source metadata
	select {
	case job := <-h.queue.Drain():
		if len(job.Pipeline.Jobs) != 0 {
			t.Errorf("expected 0 jobs in enqueued pipeline, got %d", len(job.Pipeline.Jobs))
		}
		if job.Pipeline.Name != "org/my-bootstrap-repo" {
			t.Errorf("expected pipeline name org/my-bootstrap-repo, got %s", job.Pipeline.Name)
		}
		if job.Pipeline.Source.URL != "https://github.com/org/my-bootstrap-repo.git" {
			t.Errorf("expected source URL, got %s", job.Pipeline.Source.URL)
		}
		if job.Pipeline.Source.Ref != "feature-branch" {
			t.Errorf("expected source Ref feature-branch, got %s", job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.CommitSHA != headSHA {
			t.Errorf("expected source CommitSHA %s, got %s", headSHA, job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.PRNumber != 0 {
			t.Errorf("expected source PRNumber 0, got %d", job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected job to be enqueued")
	}

	// 4. Outbox record must store valid bootstrap spec with zero jobs
	outboxItems, err := h.store.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox: %v", err)
	}
	if len(outboxItems) != 1 {
		t.Fatalf("expected 1 outbox item, got %d", len(outboxItems))
	}
	var outboxPipeline drevtypes.Pipeline
	if err := json.Unmarshal([]byte(outboxItems[0].PipelineSpec), &outboxPipeline); err != nil {
		t.Fatalf("unmarshaling outbox spec: %v", err)
	}
	if len(outboxPipeline.Jobs) != 0 {
		t.Errorf("expected 0 jobs in outbox pipeline, got %d", len(outboxPipeline.Jobs))
	}
	if outboxPipeline.Source.CommitSHA != headSHA {
		t.Errorf("expected outbox CommitSHA %s, got %s", headSHA, outboxPipeline.Source.CommitSHA)
	}
	if outboxPipeline.Source.Ref != "feature-branch" {
		t.Errorf("expected outbox Ref feature-branch, got %s", outboxPipeline.Source.Ref)
	}
	if outboxPipeline.Source.URL != "https://github.com/org/my-bootstrap-repo.git" {
		t.Errorf("expected outbox URL, got %s", outboxPipeline.Source.URL)
	}
}

func TestGitHubWebhook_PullRequest_BootstrapSpec_ZeroJobs(t *testing.T) {
	h := setupTestHandler(t, "secret", t.TempDir())

	headSHA := "fedcba9876543210fedcba9876543210fedcba98"
	payload := makePRPayload(
		"opened", 99,
		"org/pr-bootstrap-repo", "https://github.com/org/pr-bootstrap-repo.git",
		"pr-feature", headSHA,
		"Add feature", "pr-user",
		false, "org/pr-bootstrap-repo",
	)
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-pr-bootstrap-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	runID, ok := resp["run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("expected run_id in response, got %v", resp)
	}

	ctx := context.Background()

	// 1. Verify zero jobs in DB
	jobs, err := h.store.GetRunJobs(ctx, runID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 pre-registered jobs in DB for PR bootstrap run, got %d", len(jobs))
	}

	// 2. Enqueued job must have 0 jobs and authoritative Source metadata
	select {
	case job := <-h.queue.Drain():
		if len(job.Pipeline.Jobs) != 0 {
			t.Errorf("expected 0 jobs in enqueued pipeline, got %d", len(job.Pipeline.Jobs))
		}
		if job.Pipeline.Source.CommitSHA != headSHA {
			t.Errorf("expected source CommitSHA %s, got %s", headSHA, job.Pipeline.Source.CommitSHA)
		}
		if job.Pipeline.Source.Ref != "pr-feature" {
			t.Errorf("expected source Ref pr-feature, got %s", job.Pipeline.Source.Ref)
		}
		if job.Pipeline.Source.PRNumber != 99 {
			t.Errorf("expected source PRNumber 99, got %d", job.Pipeline.Source.PRNumber)
		}
	default:
		t.Fatal("expected job to be enqueued")
	}

	// 3. Outbox record contains bootstrap spec
	outboxItems, err := h.store.GetPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOutbox: %v", err)
	}
	if len(outboxItems) != 1 {
		t.Fatalf("expected 1 outbox item, got %d", len(outboxItems))
	}
	var outboxPipeline drevtypes.Pipeline
	if err := json.Unmarshal([]byte(outboxItems[0].PipelineSpec), &outboxPipeline); err != nil {
		t.Fatalf("unmarshaling outbox spec: %v", err)
	}
	if len(outboxPipeline.Jobs) != 0 {
		t.Errorf("expected 0 jobs in outbox pipeline, got %d", len(outboxPipeline.Jobs))
	}
	if outboxPipeline.Source.CommitSHA != headSHA {
		t.Errorf("expected outbox CommitSHA %s, got %s", headSHA, outboxPipeline.Source.CommitSHA)
	}
	if outboxPipeline.Source.PRNumber != 99 {
		t.Errorf("expected outbox PRNumber 99, got %d", outboxPipeline.Source.PRNumber)
	}
}

func TestGitHubWebhook_Push_MalformedCommitSHA_Rejected(t *testing.T) {
	testCases := []struct {
		name string
		sha  string
	}{
		{name: "short 7 chars", sha: "1234567"},
		{name: "short 16 chars", sha: "1234567890abcdef"},
		{name: "non-hex 40 chars", sha: "g1b2c3d4e5f678901234567890abcdef12345678"},
		{name: "dash prefix", sha: "-1b2c3d4e5f678901234567890abcdef12345678"},
		{name: "spaces inside", sha: "1234567890abcdef 1234567890abcdef1234567"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := setupTestHandler(t, "secret", t.TempDir())

			payload := PushPayload{}
			payload.Repository.FullName = "octocat/hello-world"
			payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
			payload.Ref = "refs/heads/main"
			payload.HeadCommit.ID = tc.sha
			body, _ := json.Marshal(payload)

			req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
			req.Header.Set("X-GitHub-Event", "push")
			req.Header.Set("X-GitHub-Delivery", "delivery-invalid-sha")

			rr := httptest.NewRecorder()
			h.Handle(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request, got %v: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "malformed or invalid commit SHA") {
				t.Errorf("expected error message containing 'malformed or invalid commit SHA', got: %s", rr.Body.String())
			}

			// Verify 0 runs recorded
			runs, _ := h.store.ListRuns(context.Background(), 10)
			if len(runs) != 0 {
				t.Errorf("expected 0 runs created, got %d", len(runs))
			}
		})
	}
}

func TestGitHubWebhook_BootstrapSpec_OutboxRecoveryRoundTrip(t *testing.T) {
	configDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "roundtrip.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	q := queue.New(10)
	stream := streamer.New(t.TempDir())
	h := New(s, q, parser.NewParser(), stream, "secret", configDir)

	headSHA := "a1b2c3d4e5f678901234567890abcdef12345678"
	payload := PushPayload{}
	payload.Repository.FullName = "octocat/hello-world"
	payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = headSHA
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "del-recovery-roundtrip-1")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %v: %s", rr.Code, rr.Body.String())
	}

	// Drain the initial enqueued job
	select {
	case <-q.Drain():
	default:
		t.Fatal("expected initially enqueued job")
	}

	// Now run OutboxRecoverer to verify the pending outbox entry deserializes cleanly and enqueues
	recoverer := pool.NewOutboxRecoverer(s, q, stream)
	if err := recoverer.Recover(context.Background()); err != nil {
		t.Fatalf("recoverer.Recover failed: %v", err)
	}

	// Verify recovered job was reenqueued with 0 jobs and exact Source
	select {
	case recoveredJob := <-q.Drain():
		if recoveredJob.Pipeline.Source.CommitSHA != headSHA {
			t.Errorf("expected recovered CommitSHA %s, got %s", headSHA, recoveredJob.Pipeline.Source.CommitSHA)
		}
		if len(recoveredJob.Pipeline.Jobs) != 0 {
			t.Errorf("expected 0 jobs in recovered pipeline, got %d", len(recoveredJob.Pipeline.Jobs))
		}
	default:
		t.Fatal("expected recovered job to be enqueued in queue")
	}
}
