package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type Handler struct {
	store                store.Store
	scheduler            *scheduler.Scheduler
	parser               *parser.Parser
	logDir               string
	streamer             *streamer.LogStreamer
	queue                *queue.Queue
	workers              int
	webhookHandler       *webhook.GitHubHandler
	notificationsEnabled bool
}

func New(
	store store.Store,
	scheduler *scheduler.Scheduler,
	parser *parser.Parser,
	streamer *streamer.LogStreamer,
	q *queue.Queue,
	workers int,
	webhookHandler *webhook.GitHubHandler,
	logDir string,
	notificationsEnabled bool,
) *Handler {
	return &Handler{
		store:                store,
		scheduler:            scheduler,
		parser:               parser,
		logDir:               logDir,
		streamer:             streamer,
		queue:                q,
		workers:              workers,
		webhookHandler:       webhookHandler,
		notificationsEnabled: notificationsEnabled,
	}
}

func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/api/v1/health", h.health)
	r.Get("/api/v1/queue", h.queueStatus)

	r.Post("/webhooks/github", h.webhookHandler.Handle)

	r.Group(func(r chi.Router) {
		tokensStr := os.Getenv("DREV_API_TOKENS")
		var tokens []string
		if tokensStr != "" {
			tokens = strings.Split(tokensStr, ",")
		} else {
			tokens = []string{"test-token"} // default fallback
		}

		r.Use(auth.Middleware(auth.Config{Tokens: tokens}))

		r.Post("/api/v1/pipelines/trigger", h.trigger)
		r.Get("/api/v1/runs", h.listRuns)
		r.Get("/api/v1/runs/{runID}", h.getRun)
		r.Get("/api/v1/runs/{runID}/jobs", h.getRunJobs)
		r.Get("/api/v1/runs/{runID}/logs", h.getRunLogs)
	})

	return r
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":                 "ok",
		"version":                "0.1.0",
		"notifications_enabled":  h.notificationsEnabled,
	})
}

func (h *Handler) queueStatus(w http.ResponseWriter, r *http.Request) {
	depth := 0
	if h.queue != nil {
		depth = h.queue.Depth()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"depth":   depth,
		"workers": h.workers,
		"status":  "healthy",
	})
}

type TriggerRequest struct {
	PipelinePath string            `json:"pipeline_path"`
	Env          map[string]string `json:"env"`
}

func (h *Handler) trigger(w http.ResponseWriter, r *http.Request) {
	var req TriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pipeline, err := parser.ParseFile(req.PipelinePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if pipeline.Env == nil {
		pipeline.Env = make(map[string]string)
	}
	for k, v := range req.Env {
		pipeline.Env[k] = v
	}

	runID := uuid.New().String()
	run := &drevtypes.Run{
		ID:         runID,
		PipelineID: pipeline.Name,
		Status:     drevtypes.StatusPending,
	}

	if err := h.store.CreateRun(r.Context(), run); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, job := range pipeline.Jobs {
		rj := &drevtypes.RunJob{
			ID:      uuid.New().String(),
			RunID:   runID,
			JobName: job.Name,
			Status:  drevtypes.StatusPending,
		}
		if err := h.store.CreateRunJob(r.Context(), rj); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	logW, err := h.streamer.Writer(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.queue.Enqueue(&queue.Job{
		RunID:      runID,
		Pipeline:   pipeline,
		LogWriter:  logW,
		EnqueuedAt: time.Now(),
	}); err != nil {
		logW.Close()
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"run_id": runID,
		"status": string(drevtypes.StatusPending),
	})
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
			if limit > 100 {
				limit = 100
			}
		}
	}

	runs, err := h.store.ListRuns(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if runs == nil {
		runs = []*drevtypes.Run{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(runs)
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := h.store.GetRun(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

func (h *Handler) getRunJobs(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	_, err := h.store.GetRun(r.Context(), runID)
	if err != nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	jobs, err := h.store.GetRunJobs(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobs == nil {
		jobs = []*drevtypes.RunJob{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

type sseWriter struct {
	w  http.ResponseWriter
	fl http.Flusher
}

func (sw *sseWriter) Write(p []byte) (n int, err error) {
	str := string(p)
	str = strings.TrimSuffix(str, "\n")
	if str != "" {
		fmt.Fprintf(sw.w, "data: %s\n\n", str)
		if sw.fl != nil {
			sw.fl.Flush()
		}
	}
	return len(p), nil
}

func (h *Handler) getRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	fl, _ := w.(http.Flusher)
	sw := &sseWriter{w: w, fl: fl}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run, err := h.store.GetRun(context.Background(), runID)
				if err != nil || (run.Status != drevtypes.StatusRunning && run.Status != drevtypes.StatusPending) {
					cancel()
					return
				}
			}
		}
	}()

	err := h.streamer.Tail(ctx, runID, sw)
	if err != nil && err != context.Canceled {
		// Log error silently, SSE stream already active
	}
}
