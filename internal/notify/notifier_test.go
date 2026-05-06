package notify

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func makeTestRun(status drevtypes.RunStatus) *drevtypes.Run {
	return &drevtypes.Run{
		ID:         "abcdef12-3456-7890-abcd-ef1234567890",
		PipelineID: "test-pipeline",
		Status:     status,
		StartedAt:  time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 5, 6, 10, 0, 25, 0, time.UTC),
	}
}

func makeTestPipeline() *drevtypes.Pipeline {
	return &drevtypes.Pipeline{
		Name: "test-pipeline",
	}
}

func makeTestJobs() []*drevtypes.RunJob {
	return []*drevtypes.RunJob{
		{
			ID:         "job-1",
			RunID:      "abcdef12-3456-7890-abcd-ef1234567890",
			JobName:    "test",
			Status:     drevtypes.StatusSuccess,
			StartedAt:  time.Date(2026, 5, 6, 10, 0, 2, 0, time.UTC),
			FinishedAt: time.Date(2026, 5, 6, 10, 0, 13, 0, time.UTC),
		},
		{
			ID:         "job-2",
			RunID:      "abcdef12-3456-7890-abcd-ef1234567890",
			JobName:    "build",
			Status:     drevtypes.StatusFailed,
			StartedAt:  time.Date(2026, 5, 6, 10, 0, 13, 0, time.UTC),
			FinishedAt: time.Date(2026, 5, 6, 10, 0, 18, 0, time.UTC),
		},
	}
}

func TestNotify_BothWebhooks(t *testing.T) {
	var slackCalls, discordCalls atomic.Int32

	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slackCalls.Add(1)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("slack: expected application/json content type")
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("slack: failed to decode JSON: %v", err)
		}
		if _, ok := payload["attachments"]; !ok {
			t.Error("slack: expected 'attachments' in payload")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		discordCalls.Add(1)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("discord: expected application/json content type")
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("discord: failed to decode JSON: %v", err)
		}
		if _, ok := payload["embeds"]; !ok {
			t.Error("discord: expected 'embeds' in payload")
		}
		if _, ok := payload["components"]; !ok {
			t.Error("discord: expected 'components' in payload")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer discordServer.Close()

	n := New(NotificationConfig{
		SlackWebhookURL:   slackServer.URL,
		DiscordWebhookURL: discordServer.URL,
		Enabled:           true,
	})

	err := n.NotifyPipelineComplete(
		t.Context(),
		makeTestRun(drevtypes.StatusFailed),
		makeTestPipeline(),
		makeTestJobs(),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if slackCalls.Load() != 1 {
		t.Errorf("expected 1 slack call, got %d", slackCalls.Load())
	}
	if discordCalls.Load() != 1 {
		t.Errorf("expected 1 discord call, got %d", discordCalls.Load())
	}
}

func TestNotify_SlackOnly(t *testing.T) {
	var slackCalls atomic.Int32

	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slackCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	n := New(NotificationConfig{
		SlackWebhookURL: slackServer.URL,
		Enabled:         true,
	})

	err := n.NotifyPipelineComplete(
		t.Context(),
		makeTestRun(drevtypes.StatusSuccess),
		makeTestPipeline(),
		makeTestJobs(),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if slackCalls.Load() != 1 {
		t.Errorf("expected 1 slack call, got %d", slackCalls.Load())
	}
}

func TestNotify_DiscordOnly(t *testing.T) {
	var discordCalls atomic.Int32

	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		discordCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer discordServer.Close()

	n := New(NotificationConfig{
		DiscordWebhookURL: discordServer.URL,
		Enabled:           true,
	})

	err := n.NotifyPipelineComplete(
		t.Context(),
		makeTestRun(drevtypes.StatusSuccess),
		makeTestPipeline(),
		makeTestJobs(),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if discordCalls.Load() != 1 {
		t.Errorf("expected 1 discord call, got %d", discordCalls.Load())
	}
}

func TestNotify_Timeout(t *testing.T) {
	done := make(chan struct{})
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done // Block until test signals done
	}))
	defer func() {
		close(done)
		slowServer.Close()
	}()

	n := New(NotificationConfig{
		SlackWebhookURL: slowServer.URL,
		Enabled:         true,
	})
	// Override client timeout for faster test
	n.client.Timeout = 500 * time.Millisecond

	err := n.NotifyPipelineComplete(
		t.Context(),
		makeTestRun(drevtypes.StatusSuccess),
		makeTestPipeline(),
		makeTestJobs(),
	)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestNotify_Disabled(t *testing.T) {
	n := New(NotificationConfig{
		SlackWebhookURL: "http://should-not-be-called.example.com",
		Enabled:         false,
	})

	err := n.NotifyPipelineComplete(
		t.Context(),
		makeTestRun(drevtypes.StatusSuccess),
		makeTestPipeline(),
		makeTestJobs(),
	)
	if err != nil {
		t.Fatalf("expected no error when disabled, got: %v", err)
	}
}
