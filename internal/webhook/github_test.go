package webhook

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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/pkg/drevtypes"
)

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

func TestGitHubWebhook_NoPipelineConfig(t *testing.T) {
	payload := PushPayload{}
	payload.Repository.FullName = "octocat/hello-world"
	body, _ := json.Marshal(payload)

	h := setupTestHandler(t, "secret", t.TempDir())

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected %v, got %v", http.StatusNotFound, rr.Code)
	}
}

func TestGitHubWebhook_ValidPush(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "hello-world")
	os.MkdirAll(repoDir, 0755)

	pipelineYAML := `
name: test
jobs:
  - name: test
    image: alpine
    steps:
      - name: test
        run: echo "test"
`
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(pipelineYAML), 0644)

	payload := PushPayload{}
	payload.Repository.FullName = "octocat/hello-world"
	payload.Repository.CloneURL = "https://github.com/octocat/hello-world.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = "1234567890abcdef"
	payload.Pusher.Name = "octocat"
	body, _ := json.Marshal(payload)

	h := setupTestHandler(t, "secret", configDir)

	req, _ := http.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signBody("secret", body))
	req.Header.Set("X-GitHub-Event", "push")

	rr := httptest.NewRecorder()
	h.Handle(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("expected %v, got %v", http.StatusAccepted, rr.Code)
	}

	var resp map[string]string
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp["run_id"] == "" || resp["repo"] != "octocat/hello-world" {
		t.Errorf("invalid response body: %v", resp)
	}
}

func TestGitHubWebhook_QueueFull_MarksRunAndJobsFailed(t *testing.T) {
	configDir := t.TempDir()
	repoDir := filepath.Join(configDir, "octocat", "queue-full-repo")
	os.MkdirAll(repoDir, 0755)

	pipelineYAML := `
name: test-gh-queue-full
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "test"
`
	os.WriteFile(filepath.Join(repoDir, ".drev.yml"), []byte(pipelineYAML), 0644)

	payload := PushPayload{}
	payload.Repository.FullName = "octocat/queue-full-repo"
	payload.Repository.CloneURL = "https://github.com/octocat/queue-full-repo.git"
	payload.Ref = "refs/heads/main"
	payload.HeadCommit.ID = "1234567890abcdef"
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
		if r.PipelineID == "test-gh-queue-full" {
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

	// Verify jobs are StatusFailed with finished_at
	jobs, err := h.store.GetRunJobs(context.Background(), targetRun.ID)
	if err != nil {
		t.Fatalf("getting run jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != drevtypes.StatusFailed {
		t.Errorf("expected job status %q, got %q", drevtypes.StatusFailed, jobs[0].Status)
	}
	if jobs[0].FinishedAt.IsZero() {
		t.Errorf("expected job finished_at to be populated")
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
