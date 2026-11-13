package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/drevci/drev/pkg/drevtypes"
)

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

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

	t.Setenv("DREV_API_TOKENS", "test-token")

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

func TestStats_Unauthorized(t *testing.T) {
	srv, _ := setupTestServer(t)

	res, err := http.Get(srv.URL + "/api/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %v", res.StatusCode)
	}
}

func TestStats_Authorized(t *testing.T) {
	srv, h := setupTestServer(t)
	ctx := context.Background()

	// 1. Initial empty stats check
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %v", res.StatusCode)
	}

	var emptyStats OverviewStatsResponse
	if err := json.NewDecoder(res.Body).Decode(&emptyStats); err != nil {
		t.Fatalf("decoding empty stats: %v", err)
	}
	if emptyStats.CompletedSampleCount != 0 || emptyStats.PassRate != nil || emptyStats.MeanDurationSec != nil {
		t.Errorf("expected empty stats with nil pass_rate and mean_duration, got: %+v", emptyStats)
	}
	if emptyStats.WorkerCapacity != 1 || emptyStats.QueueStatus != "healthy" {
		t.Errorf("worker_capacity = %d, queue_status = %s", emptyStats.WorkerCapacity, emptyStats.QueueStatus)
	}
	if len(emptyStats.RecentFailures) != 0 {
		t.Errorf("expected empty recent_failures, got %v", emptyStats.RecentFailures)
	}

	// 2. Populate mixed runs
	baseTime := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	// Success run: took 30s
	_ = h.store.CreateRun(ctx, &drevtypes.Run{
		ID:         "run-success",
		PipelineID: "deploy",
		Status:     drevtypes.StatusSuccess,
		StartedAt:  baseTime,
		FinishedAt: baseTime.Add(30 * time.Second),
	})
	// Failed run: crashed without finished_at
	_ = h.store.CreateRun(ctx, &drevtypes.Run{
		ID:         "run-failed-crash",
		PipelineID: "deploy",
		Status:     drevtypes.StatusFailed,
		StartedAt:  baseTime.Add(1 * time.Minute),
	})

	req2, _ := http.NewRequest("GET", srv.URL+"/api/v1/stats?limit=25", nil)
	req2.Header.Set("Authorization", "Bearer test-token")

	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %v", res2.StatusCode)
	}

	var populatedStats OverviewStatsResponse
	if err := json.NewDecoder(res2.Body).Decode(&populatedStats); err != nil {
		t.Fatalf("decoding populated stats: %v", err)
	}

	if populatedStats.CompletedSampleCount != 2 {
		t.Errorf("CompletedSampleCount = %d, want 2", populatedStats.CompletedSampleCount)
	}
	if populatedStats.SucceededCount != 1 || populatedStats.FailedCount != 1 {
		t.Errorf("SucceededCount = %d, FailedCount = %d, want 1 and 1", populatedStats.SucceededCount, populatedStats.FailedCount)
	}
	if populatedStats.PassRate == nil || *populatedStats.PassRate != 50 {
		t.Errorf("PassRate = %v, want 50", populatedStats.PassRate)
	}
	if populatedStats.DurationSampleCount != 1 {
		t.Errorf("DurationSampleCount = %d, want 1", populatedStats.DurationSampleCount)
	}
	if populatedStats.MeanDurationSec == nil || *populatedStats.MeanDurationSec != 30 {
		t.Errorf("MeanDurationSec = %v, want 30", populatedStats.MeanDurationSec)
	}
	if len(populatedStats.RecentFailures) != 1 || populatedStats.RecentFailures[0].ID != "run-failed-crash" {
		t.Errorf("RecentFailures = %+v, want run-failed-crash", populatedStats.RecentFailures)
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

func TestTrigger_QueueFull_MarksRunAndJobsFailed(t *testing.T) {
	srv, h := setupTestServer(t)

	// Fill the queue to capacity
	for i := 0; i < 10; i++ {
		if err := h.queue.Enqueue(&queue.Job{
			RunID:      fmt.Sprintf("fill-%d", i),
			Pipeline:   &drevtypes.Pipeline{Name: "fill"},
			LogWriter:  nopWriteCloser{io.Discard},
			EnqueuedAt: time.Now(),
		}); err != nil {
			t.Fatalf("filling queue at %d: %v", i, err)
		}
	}

	pipelineYAML := `
name: test-queue-full
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "test"
`
	ymlPath := filepath.Join(t.TempDir(), "queue_full.yml")
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

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable, got %v", res.StatusCode)
	}

	// Verify in store that the run is marked StatusFailed (NOT left StatusPending)
	runs, err := h.store.ListRuns(context.Background(), 50)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	var targetRun *drevtypes.Run
	for _, r := range runs {
		if r.PipelineID == "test-queue-full" {
			targetRun = r
			break
		}
	}
	if targetRun == nil {
		t.Fatal("expected run record in store, got none")
	}
	if targetRun.Status != drevtypes.StatusFailed {
		t.Errorf("expected run status %q, got %q (queue rejection must not leave pending run)", drevtypes.StatusFailed, targetRun.Status)
	}
	if targetRun.FinishedAt.IsZero() {
		t.Errorf("expected finished_at to be populated for rejected run")
	}

	// Verify all jobs in targetRun are StatusFailed with finished_at populated
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

type saasTestStore struct {
	store.Store
	orgsByTokenHash map[string]*drevtypes.Org
}

func (m *saasTestStore) GetOrgFromToken(_ context.Context, tokenHash string) (*drevtypes.Org, error) {
	if org, ok := m.orgsByTokenHash[tokenHash]; ok {
		return org, nil
	}
	return nil, store.ErrOrgNotFound
}

func setupSaaSTestServer(t *testing.T, orgsByToken map[string]*drevtypes.Org) (*httptest.Server, *Handler, store.Store) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "drev.db")
	logDir := filepath.Join(dir, "logs")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	orgsByTokenHash := make(map[string]*drevtypes.Org)
	for token, org := range orgsByToken {
		orgsByTokenHash[auth.HashToken(token)] = org
	}
	wrapper := &saasTestStore{
		Store:           s,
		orgsByTokenHash: orgsByTokenHash,
	}

	p := parser.NewParser()
	sched := scheduler.New(nil, wrapper)
	stream := streamer.New(logDir)

	t.Setenv("DREV_API_TOKENS", "static-test-token")

	q := queue.New(10)
	wh := webhook.New(wrapper, q, p, stream, "", t.TempDir())
	h := New(wrapper, sched, p, stream, q, 1, wh, logDir, false)

	server := httptest.NewServer(h.Routes())

	t.Cleanup(func() {
		server.Close()
		s.Close()
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

	return server, h, s
}

func triggerTestPipeline(t *testing.T, srvURL, token, ymlPath string) (int, string) {
	t.Helper()
	reqBody, _ := json.Marshal(map[string]interface{}{
		"pipeline_path": ymlPath,
	})
	req, err := http.NewRequest("POST", srvURL+"/api/v1/pipelines/trigger", bytes.NewBuffer(reqBody))
	if err != nil {
		t.Fatalf("creating trigger request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return res.StatusCode, string(bodyBytes)
}

func TestTrigger_ConcurrentRequests_EnforcesWorkerLimit(t *testing.T) {
	org := &drevtypes.Org{
		ID:          "org-limit-test",
		Slug:        "org-limit",
		Plan:        "pro",
		WorkerLimit: 2,
	}
	srv, _, s := setupSaaSTestServer(t, map[string]*drevtypes.Org{
		"token-limit-test": org,
	})

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: concurrent-limit-pipeline
jobs:
  - name: job-1
    image: alpine
    steps:
      - name: step-1
        run: echo "test"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	const totalRequests = 10
	type result struct {
		statusCode int
		body       string
	}
	results := make(chan result, totalRequests)

	var startWg sync.WaitGroup
	startWg.Add(1)
	var doneWg sync.WaitGroup
	doneWg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func() {
			defer doneWg.Done()
			startWg.Wait() // synchronize goroutines to hit endpoint concurrently

			status, body := triggerTestPipeline(t, srv.URL, "token-limit-test", ymlPath)
			results <- result{statusCode: status, body: body}
		}()
	}

	startWg.Done()
	doneWg.Wait()
	close(results)

	var acceptedCount, rejectedCount int
	for res := range results {
		switch res.statusCode {
		case http.StatusAccepted:
			acceptedCount++
			var m map[string]string
			if err := json.Unmarshal([]byte(res.body), &m); err != nil || m["run_id"] == "" {
				t.Errorf("expected run_id in accepted response, got %s", res.body)
			}
		case http.StatusTooManyRequests:
			rejectedCount++
			if !strings.Contains(res.body, "worker limit reached for your plan (pro: 2 workers)") {
				t.Errorf("expected 429 worker limit message, got %s", res.body)
			}
		default:
			t.Errorf("unexpected status code: %d, body: %s", res.statusCode, res.body)
		}
	}

	if acceptedCount != 2 {
		t.Errorf("expected exactly 2 accepted requests for WorkerLimit=2, got %d", acceptedCount)
	}
	if rejectedCount != totalRequests-2 {
		t.Errorf("expected exactly %d rejected requests, got %d", totalRequests-2, rejectedCount)
	}

	// Verify store state: exactly 2 runs created, both belonging to the tenant
	runs, err := s.ListRuns(context.Background(), 50)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected exactly 2 runs in store, found %d", len(runs))
	}
	for _, r := range runs {
		if r.OrgID != org.ID {
			t.Errorf("expected run.OrgID %q, got %q", org.ID, r.OrgID)
		}
		if r.Status != drevtypes.StatusPending {
			t.Errorf("expected run status %q, got %q", drevtypes.StatusPending, r.Status)
		}
	}
}

func TestTrigger_MultiTenant_IndependentAllowances(t *testing.T) {
	orgA := &drevtypes.Org{
		ID:          "org-alpha",
		Slug:        "alpha",
		Plan:        "starter",
		WorkerLimit: 2,
	}
	orgB := &drevtypes.Org{
		ID:          "org-beta",
		Slug:        "beta",
		Plan:        "pro",
		WorkerLimit: 3,
	}
	srv, _, s := setupSaaSTestServer(t, map[string]*drevtypes.Org{
		"token-alpha": orgA,
		"token-beta":  orgB,
	})

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: multi-tenant-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	// 1. Org B exhausts all 3 available workers
	for i := 1; i <= 3; i++ {
		status, body := triggerTestPipeline(t, srv.URL, "token-beta", ymlPath)
		if status != http.StatusAccepted {
			t.Fatalf("Org B run %d: expected 202, got %d (body: %s)", i, status, body)
		}
	}
	// Org B 4th run must be rejected with 429
	statusB, bodyB := triggerTestPipeline(t, srv.URL, "token-beta", ymlPath)
	if statusB != http.StatusTooManyRequests {
		t.Fatalf("Org B 4th run: expected 429, got %d (body: %s)", statusB, bodyB)
	}
	if !strings.Contains(bodyB, "worker limit reached for your plan (pro: 3 workers)") {
		t.Errorf("unexpected error message for Org B: %s", bodyB)
	}

	// 2. Org A must be completely unaffected by Org B's queue saturation
	for i := 1; i <= 2; i++ {
		status, body := triggerTestPipeline(t, srv.URL, "token-alpha", ymlPath)
		if status != http.StatusAccepted {
			t.Fatalf("Org A run %d: expected 202 despite Org B being full, got %d (body: %s)", i, status, body)
		}
	}
	// Org A 3rd run exceeds Org A's limit (2)
	statusA, bodyA := triggerTestPipeline(t, srv.URL, "token-alpha", ymlPath)
	if statusA != http.StatusTooManyRequests {
		t.Fatalf("Org A 3rd run: expected 429, got %d (body: %s)", statusA, bodyA)
	}
	if !strings.Contains(bodyA, "worker limit reached for your plan (starter: 2 workers)") {
		t.Errorf("unexpected error message for Org A: %s", bodyA)
	}

	// 3. Verify runs in store: 3 for Org B, 2 for Org A
	runs, err := s.ListRuns(context.Background(), 50)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	var countA, countB int
	for _, r := range runs {
		if r.OrgID == orgA.ID {
			countA++
		} else if r.OrgID == orgB.ID {
			countB++
		}
	}
	if countA != 2 {
		t.Errorf("expected 2 runs for Org A, got %d", countA)
	}
	if countB != 3 {
		t.Errorf("expected 3 runs for Org B, got %d", countB)
	}
}

func TestTrigger_EmptyOrgID_FailsClosed(t *testing.T) {
	emptyOrg := &drevtypes.Org{
		ID:          "",
		Slug:        "empty-org",
		Plan:        "pro",
		WorkerLimit: 2,
	}
	spaceOrg := &drevtypes.Org{
		ID:          "   ",
		Slug:        "space-org",
		Plan:        "pro",
		WorkerLimit: 2,
	}
	srv, _, s := setupSaaSTestServer(t, map[string]*drevtypes.Org{
		"token-empty": emptyOrg,
		"token-space": spaceOrg,
	})

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: empty-org-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	// Test empty OrgID
	status, body := triggerTestPipeline(t, srv.URL, "token-empty", ymlPath)
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for empty OrgID, got %d (body: %s)", status, body)
	}
	if !strings.Contains(body, "invalid tenant credentials: empty organization id") {
		t.Errorf("expected empty organization id error, got %s", body)
	}

	// Test whitespace-only OrgID
	statusSpace, bodySpace := triggerTestPipeline(t, srv.URL, "token-space", ymlPath)
	if statusSpace != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for whitespace OrgID, got %d (body: %s)", statusSpace, bodySpace)
	}
	if !strings.Contains(bodySpace, "invalid tenant credentials: empty organization id") {
		t.Errorf("expected empty organization id error, got %s", bodySpace)
	}

	// Verify no runs were created
	runs, err := s.ListRuns(context.Background(), 50)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs in store, found %d", len(runs))
	}
}

func TestTrigger_CapacityRecovery_AfterRunCompletion(t *testing.T) {
	org := &drevtypes.Org{
		ID:          "org-recovery",
		Slug:        "recovery",
		Plan:        "solo",
		WorkerLimit: 1,
	}
	srv, _, s := setupSaaSTestServer(t, map[string]*drevtypes.Org{
		"token-recovery": org,
	})

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: recovery-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	ctx := context.Background()

	// 1. First trigger succeeds
	status1, body1 := triggerTestPipeline(t, srv.URL, "token-recovery", ymlPath)
	if status1 != http.StatusAccepted {
		t.Fatalf("run 1: expected 202, got %d (body: %s)", status1, body1)
	}
	var resp1 map[string]string
	if err := json.Unmarshal([]byte(body1), &resp1); err != nil {
		t.Fatalf("unmarshaling run 1 response: %v", err)
	}
	run1ID := resp1["run_id"]
	if run1ID == "" {
		t.Fatal("expected run_id in response 1")
	}

	// 2. Second trigger is rejected (limit = 1)
	status2, body2 := triggerTestPipeline(t, srv.URL, "token-recovery", ymlPath)
	if status2 != http.StatusTooManyRequests {
		t.Fatalf("run 2: expected 429, got %d (body: %s)", status2, body2)
	}

	// 3. Mark run 1 as Success -> frees active capacity
	if err := s.UpdateRunStatus(ctx, run1ID, drevtypes.StatusSuccess); err != nil {
		t.Fatalf("updating run 1 to success: %v", err)
	}

	// 4. Trigger succeeds now
	status3, body3 := triggerTestPipeline(t, srv.URL, "token-recovery", ymlPath)
	if status3 != http.StatusAccepted {
		t.Fatalf("run 3: expected 202 after run 1 succeeded, got %d (body: %s)", status3, body3)
	}
	var resp3 map[string]string
	if err := json.Unmarshal([]byte(body3), &resp3); err != nil {
		t.Fatalf("unmarshaling run 3 response: %v", err)
	}
	run3ID := resp3["run_id"]
	if run3ID == "" {
		t.Fatal("expected run_id in response 3")
	}

	// 5. Subsequent trigger is rejected again
	status4, _ := triggerTestPipeline(t, srv.URL, "token-recovery", ymlPath)
	if status4 != http.StatusTooManyRequests {
		t.Fatalf("run 4: expected 429, got %d", status4)
	}

	// 6. Mark run 3 as Failed -> frees active capacity
	if err := s.UpdateRunStatus(ctx, run3ID, drevtypes.StatusFailed); err != nil {
		t.Fatalf("updating run 3 to failed: %v", err)
	}

	// 7. Trigger succeeds again
	status5, _ := triggerTestPipeline(t, srv.URL, "token-recovery", ymlPath)
	if status5 != http.StatusAccepted {
		t.Fatalf("run 5: expected 202 after run 3 failed, got %d", status5)
	}
}

func TestTrigger_SingleTenant_StaticToken_NoLimit(t *testing.T) {
	srv, _ := setupTestServer(t)

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: static-token-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	// Single-tenant static token should allow triggering multiple runs without tenant worker limit
	for i := 1; i <= 5; i++ {
		status, body := triggerTestPipeline(t, srv.URL, "test-token", ymlPath)
		if status != http.StatusAccepted {
			t.Fatalf("static token run %d: expected 202, got %d (body: %s)", i, status, body)
		}
	}
}

func TestTrigger_GlobalQueueFull_SaaS_FailsRunAndReleasesCapacity(t *testing.T) {
	org := &drevtypes.Org{
		ID:          "org-queue-full",
		Slug:        "queue-full",
		Plan:        "pro",
		WorkerLimit: 2,
	}
	srv, h, s := setupSaaSTestServer(t, map[string]*drevtypes.Org{
		"token-qfull": org,
	})

	// Fill the global queue to capacity (10 jobs)
	for i := 0; i < 10; i++ {
		if err := h.queue.Enqueue(&queue.Job{
			RunID:      fmt.Sprintf("fill-%d", i),
			Pipeline:   &drevtypes.Pipeline{Name: "fill"},
			LogWriter:  nopWriteCloser{io.Discard},
			EnqueuedAt: time.Now(),
		}); err != nil {
			t.Fatalf("filling queue at %d: %v", i, err)
		}
	}

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: qfull-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	// Trigger request while global queue is full -> must return 503
	status, body := triggerTestPipeline(t, srv.URL, "token-qfull", ymlPath)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d (body: %s)", status, body)
	}
	if !strings.Contains(body, "queue is full") {
		t.Errorf("expected 'queue is full' in 503 response, got %s", body)
	}

	// Check store: the rejected run must be marked StatusFailed so it doesn't leak tenant capacity
	runs, err := s.ListRuns(context.Background(), 10)
	if err != nil {
		t.Fatalf("listing runs: %v", err)
	}
	var rejectedRun *drevtypes.Run
	for _, r := range runs {
		if r.OrgID == org.ID {
			rejectedRun = r
			break
		}
	}
	if rejectedRun == nil {
		t.Fatal("expected rejected run to exist in store")
	}
	if rejectedRun.Status != drevtypes.StatusFailed {
		t.Fatalf("expected rejected run to have status %q, got %q", drevtypes.StatusFailed, rejectedRun.Status)
	}

	// Drain 1 item from the global queue
	select {
	case job := <-h.queue.Drain():
		if job.LogWriter != nil {
			job.LogWriter.Close()
		}
	case <-time.After(time.Second):
		t.Fatal("timed out draining 1 job from queue")
	}

	// Now trigger again. Because the rejected run was marked StatusFailed,
	// Org's active count is 0, so this trigger should succeed with 202!
	status2, body2 := triggerTestPipeline(t, srv.URL, "token-qfull", ymlPath)
	if status2 != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted after queue freed slot, got %d (body: %s)", status2, body2)
	}
}

type orgNotFoundStore struct {
	store.Store
}

func (m *orgNotFoundStore) CreateRunWithLimit(_ context.Context, _ *drevtypes.Run, _ []*drevtypes.RunJob, _ int) error {
	return store.ErrOrgNotFound
}

func TestTrigger_OrgNotFound_Returns401(t *testing.T) {
	org := &drevtypes.Org{
		ID:          "org-not-found",
		Slug:        "missing",
		Plan:        "pro",
		WorkerLimit: 2,
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "drev.db")
	logDir := filepath.Join(dir, "logs")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	tokenHash := auth.HashToken("token-missing")
	wrapper := &saasTestStore{
		Store: &orgNotFoundStore{Store: s},
		orgsByTokenHash: map[string]*drevtypes.Org{
			tokenHash: org,
		},
	}

	p := parser.NewParser()
	sched := scheduler.New(nil, wrapper)
	stream := streamer.New(logDir)
	q := queue.New(10)
	wh := webhook.New(wrapper, q, p, stream, "", t.TempDir())
	h := New(wrapper, sched, p, stream, q, 1, wh, logDir, false)

	server := httptest.NewServer(h.Routes())
	defer func() {
		server.Close()
		s.Close()
	}()

	ymlPath := filepath.Join(t.TempDir(), "pipeline.yml")
	pipelineYAML := `
name: test-pipeline
jobs:
  - name: test-job
    image: alpine
    steps:
      - name: test
        run: echo "ok"
`
	if err := os.WriteFile(ymlPath, []byte(pipelineYAML), 0644); err != nil {
		t.Fatalf("writing pipeline yaml: %v", err)
	}

	status, body := triggerTestPipeline(t, server.URL, "token-missing", ymlPath)
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for ErrOrgNotFound, got %d (body: %s)", status, body)
	}
	if !strings.Contains(body, "organization not found") {
		t.Errorf("expected 'organization not found' in response, got %s", body)
	}
}
