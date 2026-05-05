package api

import (
	"bytes"
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

func setupTestServer(t *testing.T) (*httptest.Server, *Handler) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "drev.db")
	logDir := filepath.Join(dir, "logs")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	p := parser.NewParser()
	sched := scheduler.New(nil, s) // Mock scheduler with nil runner
	stream := streamer.New(logDir)

	os.Setenv("DREV_API_TOKENS", "test-token")

	h := New(s, sched, p, stream, nil, logDir)
	server := httptest.NewServer(h.Routes())

	t.Cleanup(func() {
		server.Close()
		s.Close()
	})

	return server, h
}

func TestHealth(t *testing.T) {
	srv, _ := setupTestServer(t)

	res, err := http.Get(srv.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %v", res.StatusCode)
	}
}

func TestTrigger_ValidPipeline(t *testing.T) {
	srv, _ := setupTestServer(t)

	pipelineYAML := `
name: test
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "test"
`
	ymlPath := filepath.Join(t.TempDir(), "valid.yml")
	os.WriteFile(ymlPath, []byte(pipelineYAML), 0644)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"pipeline_path": ymlPath,
	})

	req, _ := http.NewRequest("POST", srv.URL+"/api/v1/pipelines/trigger", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202 Accepted, got %v", res.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(res.Body).Decode(&body)
	if body["run_id"] == "" {
		t.Errorf("expected run_id, got empty")
	}
}

func TestTrigger_InvalidPipeline(t *testing.T) {
	srv, _ := setupTestServer(t)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"pipeline_path": "/path/to/nowhere.yml",
	})

	req, _ := http.NewRequest("POST", srv.URL+"/api/v1/pipelines/trigger", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %v", res.StatusCode)
	}
}

func TestGetRun_NotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %v", res.StatusCode)
	}
}

func TestAuth_MissingToken(t *testing.T) {
	srv, _ := setupTestServer(t)

	res, err := http.Get(srv.URL + "/api/v1/runs")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %v", res.StatusCode)
	}
}

func TestAuth_InvalidToken(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer WRONGTOKEN")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %v", res.StatusCode)
	}
}
