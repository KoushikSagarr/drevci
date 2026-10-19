package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/drevci/drev/pkg/drevtypes"
)

func setupTestServer(t *testing.T) (*httptest.Server, *Handler) {
	return setupTestServerWithLogger(t, nil)
}

func setupTestServerWithLogger(t *testing.T, logger *log.Logger) (*httptest.Server, *Handler) {
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

	q := queue.New(10)
	wh := webhook.New(s, q, p, stream, "", t.TempDir())
	h := New(s, sched, p, stream, q, 1, wh, logDir, false)
	if logger != nil {
		h.SetLogger(logger)
	}
	server := httptest.NewServer(h.Routes())

	t.Cleanup(func() {
		server.Close()
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

func TestQueueStatus(t *testing.T) {
	srv, _ := setupTestServer(t)

	res, err := http.Get(srv.URL + "/api/v1/queue")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %v", res.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(res.Body).Decode(&body)
	if body["status"] != "healthy" {
		t.Errorf("expected status healthy, got %v", body["status"])
	}
}

func TestGetRunLogs_Unauthorized(t *testing.T) {
	srv, _ := setupTestServer(t)

	res, err := http.Get(srv.URL + "/api/v1/runs/test-run/logs")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %v", res.StatusCode)
	}
}

func TestGetRunLogs_Authorized(t *testing.T) {
	srv, h := setupTestServer(t)

	ctx := context.Background()
	_ = h.store.CreateRun(ctx, &drevtypes.Run{
		ID:         "test-run-logs",
		PipelineID: "test",
		Status:     drevtypes.StatusSuccess,
	})

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs/test-run-logs/logs", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %v", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("expected text/event-stream, got %v", ct)
	}
}

func TestSanitizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty query",
			input:    "",
			expected: "",
		},
		{
			name:     "benign parameters preserved",
			input:    "limit=20&offset=40&branch=main",
			expected: "limit=20&offset=40&branch=main",
		},
		{
			name:     "token lowercase",
			input:    "token=secret-123",
			expected: "token=[REDACTED]",
		},
		{
			name:     "token uppercase and mixed case",
			input:    "TOKEN=secret-123&Token=secret-456&tOkEn=secret-789",
			expected: "TOKEN=[REDACTED]&Token=[REDACTED]&tOkEn=[REDACTED]",
		},
		{
			name:     "common credential parameters",
			input:    "access_token=s1&api_key=s2&apiKey=s3&api-key=s4&secret=s5&password=s6&auth=s7",
			expected: "access_token=[REDACTED]&api_key=[REDACTED]&apiKey=[REDACTED]&api-key=[REDACTED]&secret=[REDACTED]&password=[REDACTED]&auth=[REDACTED]",
		},
		{
			name:     "mixed sensitive and benign parameters",
			input:    "limit=20&token=sensitive-val&branch=main&secret=pass",
			expected: "limit=20&token=[REDACTED]&branch=main&secret=[REDACTED]",
		},
		{
			name:     "parameter without value",
			input:    "token&limit=20",
			expected: "token=[REDACTED]&limit=20",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeQuery(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeQuery(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestRequestLogging_QueryTokenRedacted(t *testing.T) {
	var logBuf bytes.Buffer
	testLogger := log.New(&logBuf, "", 0)

	srv, _ := setupTestServerWithLogger(t, testLogger)

	sentinel := "super-secret-sentinel-xyz-987"
	// Use unauthenticated /api/v1/health endpoint to prove redaction happens inside logger
	// rather than by authentication rejecting the request before logging.
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/health?token="+sentinel+"&limit=20", nil)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}

	loggedOutput := logBuf.String()

	if strings.Contains(loggedOutput, sentinel) {
		t.Fatalf("SECURITY LEAK: sentinel value %q was found in request log:\n%s", sentinel, loggedOutput)
	}
	if !strings.Contains(loggedOutput, "token=[REDACTED]") {
		t.Errorf("expected 'token=[REDACTED]' in log output, got:\n%s", loggedOutput)
	}
	if !strings.Contains(loggedOutput, "limit=20") {
		t.Errorf("expected benign parameter 'limit=20' in log output, got:\n%s", loggedOutput)
	}
}

func TestRequestLogging_CaseVariationsAndSensitiveKeys(t *testing.T) {
	var logBuf bytes.Buffer
	testLogger := log.New(&logBuf, "", 0)

	srv, _ := setupTestServerWithLogger(t, testLogger)

	sentinels := map[string]string{
		"token":        "tok-sentinel-111",
		"TOKEN":        "tok-sentinel-222",
		"Token":        "tok-sentinel-333",
		"access_token": "tok-sentinel-444",
		"api_key":      "tok-sentinel-555",
		"secret":       "tok-sentinel-666",
		"password":     "tok-sentinel-777",
	}

	var queryParts []string
	for k, v := range sentinels {
		queryParts = append(queryParts, k+"="+v)
	}
	queryParts = append(queryParts, "benign_param=keep_me")

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/health?"+strings.Join(queryParts, "&"), nil)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	loggedOutput := logBuf.String()

	for k, v := range sentinels {
		if strings.Contains(loggedOutput, v) {
			t.Errorf("SECURITY LEAK: parameter %q leaked sentinel value %q in log:\n%s", k, v, loggedOutput)
		}
		if !strings.Contains(loggedOutput, k+"=[REDACTED]") {
			t.Errorf("expected %q to be redacted as %s=[REDACTED] in log:\n%s", k, k, loggedOutput)
		}
	}

	if !strings.Contains(loggedOutput, "benign_param=keep_me") {
		t.Errorf("expected benign parameter to be preserved in log output, got:\n%s", loggedOutput)
	}
}

func TestRequestLogging_HandlerReceivesOriginalQuery(t *testing.T) {
	var logBuf bytes.Buffer
	testLogger := log.New(&logBuf, "", 0)

	srv, _ := setupTestServerWithLogger(t, testLogger)

	sentinel := "sentinel-for-handler-check"
	// Request /api/v1/runs with limit=1 and token=<sentinel>, using valid Authorization header
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs?limit=1&token="+sentinel, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}

	// Verify log redacts sentinel
	loggedOutput := logBuf.String()
	if strings.Contains(loggedOutput, sentinel) {
		t.Fatalf("SECURITY LEAK: sentinel leaked in log: %s", loggedOutput)
	}
	if !strings.Contains(loggedOutput, "token=[REDACTED]") {
		t.Errorf("expected token=[REDACTED] in log, got: %s", loggedOutput)
	}
	if !strings.Contains(loggedOutput, "limit=1") {
		t.Errorf("expected limit=1 in log, got: %s", loggedOutput)
	}
}

func TestAuth_QueryTokenAloneIsRejectedAtAPI(t *testing.T) {
	var logBuf bytes.Buffer
	testLogger := log.New(&logBuf, "", 0)

	srv, _ := setupTestServerWithLogger(t, testLogger)

	// Attempt authentication solely using query string ?token=test-token
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/runs?token=test-token", nil)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SECURITY DEFECT: query token alone was accepted by API, got status %d", res.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(res.Body).Decode(&body)
	if body["error"] != "missing token" {
		t.Errorf("expected 'missing token', got %q", body["error"])
	}

	// Verify logs redact the token value
	loggedOutput := logBuf.String()
	if strings.Contains(loggedOutput, "test-token") {
		t.Fatalf("SECURITY LEAK: token leaked in log during rejected query-token request: %s", loggedOutput)
	}
	if !strings.Contains(loggedOutput, "token=[REDACTED]") {
		t.Errorf("expected token=[REDACTED] in log, got: %s", loggedOutput)
	}
}
