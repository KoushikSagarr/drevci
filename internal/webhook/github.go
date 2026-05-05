package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
)

type GitHubHandler struct {
	store     store.Store
	queue     *queue.Queue
	parser    *parser.Parser
	streamer  *streamer.LogStreamer
	secret    string
	configDir string
}

func New(
	store store.Store,
	q *queue.Queue,
	parser *parser.Parser,
	streamer *streamer.LogStreamer,
	secret string,
	configDir string,
) *GitHubHandler {
	return &GitHubHandler{
		store:     store,
		queue:     q,
		parser:    parser,
		streamer:  streamer,
		secret:    secret,
		configDir: configDir,
	}
}

type PushPayload struct {
	Ref        string `json:"ref"`
	Repository struct {
		FullName      string `json:"full_name"`
		CloneURL      string `json:"clone_url"`
		DefaultBranch string `json:"default_branch"`
	} `json:"repository"`
	HeadCommit struct {
		ID      string `json:"id"`
		Message string `json:"message"`
		Author  struct {
			Name string `json:"name"`
		} `json:"author"`
	} `json:"head_commit"`
	Pusher struct {
		Name string `json:"name"`
	} `json:"pusher"`
}

func (h *GitHubHandler) Handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}

	sig := r.Header.Get("X-Hub-Signature-256")
	if sig == "" {
		http.Error(w, "missing signature", http.StatusBadRequest)
		return
	}

	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	log.Printf("received sig: %s", sig)
	log.Printf("expected sig: %s", expected)

	if !hmac.Equal([]byte(sig), []byte(expected)) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	event := r.Header.Get("X-GitHub-Event")
	if event != "push" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("event ignored"))
		return
	}

	var payload PushPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid payload JSON", http.StatusBadRequest)
		return
	}

	branch := strings.TrimPrefix(payload.Ref, "refs/heads/")
	if branch == payload.Ref {
		branch = strings.TrimPrefix(payload.Ref, "refs/tags/")
	}

	configPath := filepath.Join(h.configDir, payload.Repository.FullName, ".drev.yml")
	pipeline, err := parser.ParseFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "no such file or directory") || strings.Contains(err.Error(), "cannot find the path") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no pipeline configured for this repo"})
			return
		}
		http.Error(w, fmt.Sprintf("failed to parse pipeline: %v", err), http.StatusInternalServerError)
		return
	}

	pipeline.Source.URL = payload.Repository.CloneURL
	pipeline.Source.Ref = branch

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

	shortSHA := payload.HeadCommit.ID
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"run_id":       runID,
		"repo":         payload.Repository.FullName,
		"branch":       branch,
		"commit":       shortSHA,
		"triggered_by": payload.Pusher.Name,
	})
}
