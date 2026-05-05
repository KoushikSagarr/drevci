package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
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
	t.Cleanup(func() { s.Close() })

	p := parser.NewParser()
	sched := scheduler.New(nil, s)
	stream := streamer.New(t.TempDir())

	return New(s, sched, p, stream, secret, configDir)
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
