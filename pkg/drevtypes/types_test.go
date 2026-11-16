package drevtypes

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRun_JSONSerialization_NullCancellationRequestedAt(t *testing.T) {
	run := &Run{
		ID:           "run-json-1",
		PipelineID:   "pipeline-1",
		Status:       StatusPending,
		TriggeredBy:  "api",
		StartedAt:    time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC),
		FinishedAt:   time.Date(2026, 11, 15, 10, 5, 0, 0, time.UTC),
		CancellationRequestedAt: nil,
	}

	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	jsonStr := string(data)
	if strings.Contains(jsonStr, "cancellation_requested_at") {
		t.Errorf("expected cancellation_requested_at to be omitted from JSON, got: %s", jsonStr)
	}

	var decoded Run
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.CancellationRequestedAt != nil {
		t.Errorf("expected decoded.CancellationRequestedAt to be nil, got: %v", decoded.CancellationRequestedAt)
	}
}

func TestRun_JSONSerialization_PopulatedCancellationRequestedAt(t *testing.T) {
	cancelTime := time.Date(2026, 11, 15, 12, 34, 56, 0, time.UTC)
	run := &Run{
		ID:                      "run-json-2",
		PipelineID:              "pipeline-2",
		Status:                  StatusRunning,
		TriggeredBy:             "api",
		StartedAt:               time.Date(2026, 11, 15, 12, 30, 0, 0, time.UTC),
		CancellationRequestedAt: &cancelTime,
	}

	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"cancellation_requested_at":"2026-11-15T12:34:56Z"`) {
		t.Errorf("expected RFC3339 formatted cancellation_requested_at in JSON, got: %s", jsonStr)
	}

	var decoded Run
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.CancellationRequestedAt == nil {
		t.Fatal("expected decoded.CancellationRequestedAt to be non-nil")
	}

	if !decoded.CancellationRequestedAt.Equal(cancelTime) {
		t.Errorf("expected %v, got %v", cancelTime, *decoded.CancellationRequestedAt)
	}
}

func TestRun_JSONDeserialization_NullAndMissingFields(t *testing.T) {
	// Case 1: Explicit null in JSON
	payloadWithNull := `{"id":"run-3","pipeline_id":"p3","status":"running","cancellation_requested_at":null}`
	var runNull Run
	if err := json.Unmarshal([]byte(payloadWithNull), &runNull); err != nil {
		t.Fatalf("unmarshal explicit null failed: %v", err)
	}
	if runNull.CancellationRequestedAt != nil {
		t.Errorf("expected nil CancellationRequestedAt for explicit null, got %v", runNull.CancellationRequestedAt)
	}

	// Case 2: Missing field in legacy JSON payload
	legacyPayload := `{"id":"run-4","pipeline_id":"p4","status":"success"}`
	var runLegacy Run
	if err := json.Unmarshal([]byte(legacyPayload), &runLegacy); err != nil {
		t.Fatalf("unmarshal legacy payload failed: %v", err)
	}
	if runLegacy.CancellationRequestedAt != nil {
		t.Errorf("expected nil CancellationRequestedAt for legacy payload, got %v", runLegacy.CancellationRequestedAt)
	}
}
