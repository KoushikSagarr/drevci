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
	return &drevtypes.Pipeline{Name: "test-pipeline"}
}

func makeTestJobs() []*drevtypes.RunJob {
	return []*drevtypes.RunJob{
		{
			ID: "job-1", RunID: "abcdef12-3456-7890-abcd-ef1234567890",
			JobName: "test", Status: drevtypes.StatusSuccess,
			StartedAt:  time.Date(2026, 5, 6, 10, 0, 2, 0, time.UTC),
			FinishedAt: time.Date(2026, 5, 6, 10, 0, 13, 0, time.UTC),
		},
		{
			ID: "job-2", RunID: "abcdef12-3456-7890-abcd-ef1234567890",
			JobName: "build", Status: drevtypes.StatusFailed,
			StartedAt:  time.Date(2026, 5, 6, 10, 0, 13, 0, time.UTC),
			FinishedAt: time.Date(2026, 5, 6, 10, 0, 18, 0, time.UTC),
		},
	}
}

func TestNotify_BothWebhooks(t *testing.T) {
	var slackCalls, discordCalls atomic.Int32
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slackCalls.Add(1)
		var p map[string]interface{}
		json.NewDecoder(r.Body).Decode(&p)
		if _, ok := p["attachments"]; !ok {
			t.Error("slack: missing attachments")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()
	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		discordCalls.Add(1)
		var p map[string]interface{}
		json.NewDecoder(r.Body).Decode(&p)
		if _, ok := p["embeds"]; !ok {
			t.Error("discord: missing embeds")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer discordServer.Close()

	n := New(NotificationConfig{SlackWebhookURL: slackServer.URL, DiscordWebhookURL: discordServer.URL, Enabled: true})
	err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusFailed), makeTestPipeline(), makeTestJobs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slackCalls.Load() != 1 {
		t.Errorf("slack calls: got %d, want 1", slackCalls.Load())
	}
	if discordCalls.Load() != 1 {
		t.Errorf("discord calls: got %d, want 1", discordCalls.Load())
	}
}

func TestNotify_SlackOnly(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := New(NotificationConfig{SlackWebhookURL: srv.URL, Enabled: true})
	if err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusSuccess), makeTestPipeline(), makeTestJobs()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls: got %d, want 1", calls.Load())
	}
}

func TestNotify_DiscordOnly(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	n := New(NotificationConfig{DiscordWebhookURL: srv.URL, Enabled: true})
	if err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusSuccess), makeTestPipeline(), makeTestJobs()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls: got %d, want 1", calls.Load())
	}
}

func TestNotify_Timeout(t *testing.T) {
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
	}))
	defer func() { close(done); srv.Close() }()

	n := New(NotificationConfig{SlackWebhookURL: srv.URL, Enabled: true})
	n.client.Timeout = 500 * time.Millisecond
	err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusSuccess), makeTestPipeline(), makeTestJobs())
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestNotify_Disabled(t *testing.T) {
	n := New(NotificationConfig{SlackWebhookURL: "http://should-not-call.example.com", Enabled: false})
	if err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusSuccess), makeTestPipeline(), makeTestJobs()); err != nil {
		t.Fatal(err)
	}
}

func TestNotify_Cancelled(t *testing.T) {
	var slackPayload, discordPayload map[string]interface{}
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&slackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&discordPayload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer discordServer.Close()

	n := New(NotificationConfig{
		SlackWebhookURL:   slackServer.URL,
		DiscordWebhookURL: discordServer.URL,
		Enabled:           true,
	})

	jobs := []*drevtypes.RunJob{
		{
			ID: "j1", RunID: "run-cancel", JobName: "job-cancel", Status: drevtypes.StatusCancelled,
		},
		{
			ID: "j2", RunID: "run-cancel", JobName: "job-skip", Status: drevtypes.StatusSkipped,
		},
	}
	err := n.NotifyPipelineComplete(t.Context(), makeTestRun(drevtypes.StatusCancelled), makeTestPipeline(), jobs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify Slack payload contains Cancelled
	attachments, ok := slackPayload["attachments"].([]interface{})
	if !ok || len(attachments) == 0 {
		t.Fatalf("slack missing attachments")
	}
	firstAtt := attachments[0].(map[string]interface{})
	if firstAtt["color"] != "#f39c12" {
		t.Errorf("slack color = %v, want #f39c12", firstAtt["color"])
	}
	blocks := firstAtt["blocks"].([]interface{})
	firstBlock := blocks[0].(map[string]interface{})
	textMap := firstBlock["text"].(map[string]interface{})
	if textMap["text"] != "*Pipeline: test-pipeline*\nStatus: :warning: *Cancelled*" {
		t.Errorf("slack text = %v, want Cancelled status", textMap["text"])
	}

	// Verify Discord payload contains Cancelled
	embeds, ok := discordPayload["embeds"].([]interface{})
	if !ok || len(embeds) == 0 {
		t.Fatalf("discord missing embeds")
	}
	firstEmbed := embeds[0].(map[string]interface{})
	if firstEmbed["color"].(float64) != 15967250 {
		t.Errorf("discord color = %v, want 15967250", firstEmbed["color"])
	}
	fields := firstEmbed["fields"].([]interface{})
	foundStatus := false
	for _, f := range fields {
		fieldMap := f.(map[string]interface{})
		if fieldMap["name"] == "Status" {
			foundStatus = true
			if fieldMap["value"] != "Cancelled" {
				t.Errorf("discord status = %v, want Cancelled", fieldMap["value"])
			}
		}
	}
	if !foundStatus {
		t.Error("discord missing Status field")
	}
}
