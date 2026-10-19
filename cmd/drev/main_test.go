package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func TestPollRunStatus_ImmediateSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/test-run" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drevtypes.Run{
			ID:     "test-run",
			Status: drevtypes.StatusSuccess,
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code, err := PollRunStatus(context.Background(), PollOptions{
		Client:       server.Client(),
		ServerURL:    server.URL,
		RunID:        "test-run",
		PollInterval: 5 * time.Millisecond,
		Timeout:      2 * time.Second,
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
		t.Errorf("expected stdout to contain '✓ Pipeline succeeded', got: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("expected empty stderr, got: %q", stderr.String())
	}
}

func TestPollRunStatus_PendingRunningSuccess(t *testing.T) {
	var pollCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&pollCount, 1)
		var status drevtypes.RunStatus
		switch count {
		case 1:
			status = drevtypes.StatusPending
		case 2:
			status = drevtypes.StatusRunning
		default:
			status = drevtypes.StatusSuccess
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drevtypes.Run{
			ID:     "test-run-progression",
			Status: status,
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code, err := PollRunStatus(context.Background(), PollOptions{
		Client:       server.Client(),
		ServerURL:    server.URL,
		RunID:        "test-run-progression",
		PollInterval: 10 * time.Millisecond,
		Timeout:      2 * time.Second,
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if atomic.LoadInt32(&pollCount) < 3 {
		t.Errorf("expected at least 3 polls before success, got %d", atomic.LoadInt32(&pollCount))
	}
	if !strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
		t.Errorf("expected stdout to contain '✓ Pipeline succeeded', got: %q", stdout.String())
	}
}

func TestPollRunStatus_Failed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drevtypes.Run{
			ID:     "test-run-fail",
			Status: drevtypes.StatusFailed,
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code, _ := PollRunStatus(context.Background(), PollOptions{
		Client:       server.Client(),
		ServerURL:    server.URL,
		RunID:        "test-run-fail",
		PollInterval: 5 * time.Millisecond,
		Timeout:      2 * time.Second,
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if code != 1 {
		t.Errorf("expected exit code 1 for failed run, got %d", code)
	}
	if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
		t.Errorf("SAFETY VIOLATION: reported success on failed run: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "✗ Pipeline failed (status: failed)") {
		t.Errorf("expected failure message in stderr, got: %q", stderr.String())
	}
}

func TestPollRunStatus_Cancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drevtypes.Run{
			ID:     "test-run-cancel",
			Status: drevtypes.StatusCancelled,
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code, _ := PollRunStatus(context.Background(), PollOptions{
		Client:       server.Client(),
		ServerURL:    server.URL,
		RunID:        "test-run-cancel",
		PollInterval: 5 * time.Millisecond,
		Timeout:      2 * time.Second,
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if code != 1 {
		t.Errorf("expected exit code 1 for cancelled run, got %d", code)
	}
	if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
		t.Errorf("SAFETY VIOLATION: reported success on cancelled run: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "✗ Pipeline cancelled (status: cancelled)") {
		t.Errorf("expected cancellation message in stderr, got: %q", stderr.String())
	}
}

func TestPollRunStatus_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code, err := PollRunStatus(context.Background(), PollOptions{
		Client:       server.Client(),
		ServerURL:    server.URL,
		RunID:        "test-run-err",
		PollInterval: 5 * time.Millisecond,
		Timeout:      2 * time.Second,
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if code != 1 {
		t.Errorf("expected exit code 1 on API error, got %d", code)
	}
	if err == nil {
		t.Errorf("expected non-nil error on API error, got nil")
	}
	if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
		t.Errorf("SAFETY VIOLATION: reported success on API error: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "Error checking status:") {
		t.Errorf("expected error message in stderr, got: %q", stderr.String())
	}
}

func TestPollRunStatus_UnknownOrMalformedStatus(t *testing.T) {
	t.Run("unknown status string", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"id":"test-unknown","status":"unknown_state_xyz"}`))
		}))
		defer server.Close()

		var stdout, stderr bytes.Buffer
		code, err := PollRunStatus(context.Background(), PollOptions{
			Client:       server.Client(),
			ServerURL:    server.URL,
			RunID:        "test-unknown",
			PollInterval: 5 * time.Millisecond,
			Timeout:      2 * time.Second,
			Out:          &stdout,
			ErrOut:       &stderr,
		})

		if code != 1 {
			t.Errorf("expected exit code 1 on unknown status, got %d", code)
		}
		if err == nil {
			t.Errorf("expected error on unknown status, got nil")
		}
		if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
			t.Errorf("SAFETY VIOLATION: reported success on unknown status: %q", stdout.String())
		}
		if !strings.Contains(stderr.String(), `✗ Pipeline completed with unexpected status: "unknown_state_xyz"`) {
			t.Errorf("expected unexpected status message in stderr, got: %q", stderr.String())
		}
	})

	t.Run("malformed json response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{not valid json`))
		}))
		defer server.Close()

		var stdout, stderr bytes.Buffer
		code, err := PollRunStatus(context.Background(), PollOptions{
			Client:       server.Client(),
			ServerURL:    server.URL,
			RunID:        "test-malformed",
			PollInterval: 5 * time.Millisecond,
			Timeout:      2 * time.Second,
			Out:          &stdout,
			ErrOut:       &stderr,
		})

		if code != 1 {
			t.Errorf("expected exit code 1 on malformed JSON, got %d", code)
		}
		if err == nil {
			t.Errorf("expected decode error, got nil")
		}
		if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
			t.Errorf("SAFETY VIOLATION: reported success on malformed JSON: %q", stdout.String())
		}
	})
}

func TestPollRunStatus_TimeoutAndCancellation(t *testing.T) {
	t.Run("timeout exceeded", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(drevtypes.Run{
				ID:     "test-stuck-running",
				Status: drevtypes.StatusRunning,
			})
		}))
		defer server.Close()

		var stdout, stderr bytes.Buffer
		code, err := PollRunStatus(context.Background(), PollOptions{
			Client:       server.Client(),
			ServerURL:    server.URL,
			RunID:        "test-stuck-running",
			PollInterval: 5 * time.Millisecond,
			Timeout:      25 * time.Millisecond,
			Out:          &stdout,
			ErrOut:       &stderr,
		})

		if code != 1 {
			t.Errorf("expected exit code 1 on timeout, got %d", code)
		}
		if err == nil {
			t.Errorf("expected error on timeout, got nil")
		}
		if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
			t.Errorf("SAFETY VIOLATION: reported success on timeout: %q", stdout.String())
		}
		if !strings.Contains(stderr.String(), "✗ Pipeline timed out waiting for completion") {
			t.Errorf("expected timeout message in stderr, got: %q", stderr.String())
		}
	})

	t.Run("context cancelled externally", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(drevtypes.Run{
				ID:     "test-cancelled-context",
				Status: drevtypes.StatusRunning,
			})
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately after first check
		time.AfterFunc(15*time.Millisecond, cancel)

		var stdout, stderr bytes.Buffer
		code, err := PollRunStatus(ctx, PollOptions{
			Client:       server.Client(),
			ServerURL:    server.URL,
			RunID:        "test-cancelled-context",
			PollInterval: 5 * time.Millisecond,
			Timeout:      5 * time.Second,
			Out:          &stdout,
			ErrOut:       &stderr,
		})

		if code != 1 {
			t.Errorf("expected exit code 1 on context cancellation, got %d", code)
		}
		if err == nil {
			t.Errorf("expected error on cancellation, got nil")
		}
		if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
			t.Errorf("SAFETY VIOLATION: reported success on context cancellation: %q", stdout.String())
		}
		if !strings.Contains(stderr.String(), "✗ Pipeline wait interrupted / cancelled") {
			t.Errorf("expected interrupted message in stderr, got: %q", stderr.String())
		}
	})
}

func TestPollRunStatus_NeverPrematureSuccessOnPendingOrRunning(t *testing.T) {
	for _, nonTerminalStatus := range []drevtypes.RunStatus{drevtypes.StatusPending, drevtypes.StatusRunning} {
		t.Run(string(nonTerminalStatus), func(t *testing.T) {
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&calls, 1)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(drevtypes.Run{
					ID:     "test-nonterminal",
					Status: nonTerminalStatus,
				})
			}))
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			// After 4 polls, cancel context to stop test
			go func() {
				for atomic.LoadInt32(&calls) < 4 {
					time.Sleep(2 * time.Millisecond)
				}
				cancel()
			}()

			var stdout, stderr bytes.Buffer
			code, _ := PollRunStatus(ctx, PollOptions{
				Client:       server.Client(),
				ServerURL:    server.URL,
				RunID:        "test-nonterminal",
				PollInterval: 5 * time.Millisecond,
				Timeout:      5 * time.Second,
				Out:          &stdout,
				ErrOut:       &stderr,
			})

			if code == 0 {
				t.Fatalf("CRITICAL DEFECT: status %s returned exit code 0 prematurely!", nonTerminalStatus)
			}
			if strings.Contains(stdout.String(), "✓ Pipeline succeeded") {
				t.Fatalf("CRITICAL DEFECT: status %s reported '✓ Pipeline succeeded' prematurely!", nonTerminalStatus)
			}
			if atomic.LoadInt32(&calls) < 4 {
				t.Errorf("expected at least 4 polls while in %s status, got %d", nonTerminalStatus, atomic.LoadInt32(&calls))
			}
		})
	}
}

func TestExecuteRun_FullFlow(t *testing.T) {
	var pollCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/pipelines/trigger":
			if r.Method != "POST" {
				http.Error(w, "invalid method", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{
				"run_id": "r-full-flow",
			})

		case "/api/v1/runs/r-full-flow/logs":
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte("data: [drev] build started\n\n"))
			w.Write([]byte("data: [drev] build finished\n\n"))

		case "/api/v1/runs/r-full-flow":
			count := atomic.AddInt32(&pollCount, 1)
			status := drevtypes.StatusRunning
			if count >= 2 {
				status = drevtypes.StatusSuccess
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(drevtypes.Run{
				ID:     "r-full-flow",
				Status: status,
			})

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := ExecuteRun(context.Background(), RunConfig{
		ServerURL:    server.URL,
		PipelinePath: "test.drev.yml",
		Follow:       true,
		Timeout:      2 * time.Second,
		PollInterval: 10 * time.Millisecond,
		Client:       server.Client(),
		Out:          &stdout,
		ErrOut:       &stderr,
	})

	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr=%q", code, stderr.String())
	}
	outStr := stdout.String()
	if !strings.Contains(outStr, "▶ Run started: r-full-flow") {
		t.Errorf("expected run started message, got: %q", outStr)
	}
	if !strings.Contains(outStr, "[drev] build started") {
		t.Errorf("expected log streaming in stdout, got: %q", outStr)
	}
	if !strings.Contains(outStr, "✓ Pipeline succeeded") {
		t.Errorf("expected success message, got: %q", outStr)
	}
}
