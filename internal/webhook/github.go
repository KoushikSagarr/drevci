package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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
	After      string `json:"after"`
	Repository struct {
		FullName      string `json:"full_name"`
		CloneURL      string `json:"clone_url"`
		DefaultBranch string `json:"default_branch"`
		Fork          bool   `json:"fork"`
	} `json:"repository"`
	HeadCommit struct {
		ID      string `json:"id"`
		Message string `json:"message"`
		Author  struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"author"`
	} `json:"head_commit"`
	Pusher struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	} `json:"pusher"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
}

type PullRequestPayload struct {
	Action      string `json:"action"` // "opened", "synchronize", "reopened", etc.
	Number      int    `json:"number"`
	PullRequest struct {
		ID     int64  `json:"id"`
		Number int    `json:"number"`
		State  string `json:"state"`
		Title  string `json:"title"`
		User   struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			Ref  string `json:"ref"`
			SHA  string `json:"sha"`
			Repo struct {
				FullName string `json:"full_name"`
				CloneURL string `json:"clone_url"`
				Fork     bool   `json:"fork"`
			} `json:"repo"`
		} `json:"head"`
		Base struct {
			Ref  string `json:"ref"`
			SHA  string `json:"sha"`
			Repo struct {
				FullName string `json:"full_name"`
				CloneURL string `json:"clone_url"`
			} `json:"repo"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository struct {
		FullName      string `json:"full_name"`
		CloneURL      string `json:"clone_url"`
		DefaultBranch string `json:"default_branch"`
		Fork          bool   `json:"fork"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
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

	if !hmac.Equal([]byte(sig), []byte(expected)) {
		log.Printf("[webhook] signature mismatch from %s", r.RemoteAddr)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	event := r.Header.Get("X-GitHub-Event")
	if event != "push" && event != "pull_request" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("event ignored"))
		return
	}

	deliveryID := strings.TrimSpace(r.Header.Get("X-GitHub-Delivery"))
	if deliveryID == "" {
		http.Error(w, "missing delivery id", http.StatusBadRequest)
		return
	}

	var (
		repoFullName string
		cloneURL     string
		branch       string
		sourceRef    string
		commitSHA    string
		commitMsg    string
		triggeredBy  string
		prNumber     int
	)

	switch event {
	case "push":
		var payload PushPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid payload JSON", http.StatusBadRequest)
			return
		}

		commitSHA = payload.HeadCommit.ID
		if commitSHA == "" {
			commitSHA = payload.After
		}

		if payload.Repository.FullName == "" || payload.Repository.CloneURL == "" || payload.Ref == "" || commitSHA == "" {
			http.Error(w, "malformed or incomplete push payload", http.StatusBadRequest)
			return
		}

		if !drevtypes.IsCommitSHA(commitSHA) {
			http.Error(w, "malformed or invalid commit SHA", http.StatusBadRequest)
			return
		}

		branch = strings.TrimPrefix(payload.Ref, "refs/heads/")
		if branch == payload.Ref {
			branch = strings.TrimPrefix(payload.Ref, "refs/tags/")
		}

		repoFullName = payload.Repository.FullName
		cloneURL = payload.Repository.CloneURL
		sourceRef = branch
		commitMsg = payload.HeadCommit.Message
		triggeredBy = payload.Pusher.Name
		if triggeredBy == "" {
			triggeredBy = payload.Sender.Login
		}
		prNumber = 0

	case "pull_request":
		var payload PullRequestPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid payload JSON", http.StatusBadRequest)
			return
		}

		switch payload.Action {
		case "opened", "synchronize", "reopened":
			// Supported pull request actions
		default:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("event ignored"))
			return
		}

		prNumber = payload.Number
		if prNumber <= 0 {
			prNumber = payload.PullRequest.Number
		}

		repoFullName = payload.Repository.FullName
		if repoFullName == "" {
			repoFullName = payload.PullRequest.Base.Repo.FullName
		}

		cloneURL = payload.Repository.CloneURL
		if cloneURL == "" {
			cloneURL = payload.PullRequest.Head.Repo.CloneURL
		}
		if cloneURL == "" {
			cloneURL = payload.PullRequest.Base.Repo.CloneURL
		}

		// Conservative default-deny policy for fork PRs: do not execute external fork PR workloads
		isFork := payload.PullRequest.Head.Repo.Fork ||
			(payload.PullRequest.Head.Repo.FullName != "" && repoFullName != "" && payload.PullRequest.Head.Repo.FullName != repoFullName)
		if isFork {
			log.Printf("[webhook] fork PR #%d from %s rejected by policy", prNumber, payload.PullRequest.Head.Repo.FullName)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "fork pull requests are not permitted by policy",
			})
			return
		}

		if payload.PullRequest.Head.SHA == "" ||
			!drevtypes.IsCommitSHA(payload.PullRequest.Head.SHA) ||
			payload.PullRequest.Head.Ref == "" ||
			strings.HasPrefix(payload.PullRequest.Head.Ref, "-") ||
			repoFullName == "" ||
			cloneURL == "" ||
			prNumber <= 0 {
			http.Error(w, "malformed or incomplete pull_request payload", http.StatusBadRequest)
			return
		}

		branch = strings.TrimPrefix(payload.PullRequest.Head.Ref, "refs/heads/")
		sourceRef = payload.PullRequest.Head.Ref
		commitSHA = payload.PullRequest.Head.SHA
		commitMsg = strings.TrimSpace(payload.PullRequest.Title)
		triggeredBy = payload.PullRequest.User.Login
		if triggeredBy == "" {
			triggeredBy = payload.Sender.Login
		}
	}

	if strings.Contains(repoFullName, "..") {
		http.Error(w, "invalid repository name", http.StatusBadRequest)
		return
	}

	pipeline := &drevtypes.Pipeline{
		Name: repoFullName,
		Source: drevtypes.Source{
			Type:      "git",
			URL:       cloneURL,
			Ref:       sourceRef,
			CommitSHA: commitSHA,
			PRNumber:  prNumber,
		},
		Jobs: nil,
	}

	runID := uuid.New().String()

	delivery := &drevtypes.WebhookDelivery{
		DeliveryID: deliveryID,
		EventType:  event,
		Repo:       repoFullName,
		PRNumber:   prNumber,
		RunID:      runID,
		CreatedAt:  time.Now().UTC(),
	}

	run := &drevtypes.Run{
		ID:          runID,
		PipelineID:  pipeline.Name,
		Status:      drevtypes.StatusPending,
		CommitSHA:   commitSHA,
		CommitMsg:   commitMsg,
		Branch:      branch,
		TriggeredBy: triggeredBy,
	}

	// Atomically persist delivery claim, run, zero jobs, and bootstrap pipeline outbox spec in a single database transaction
	pipelineSpecBytes, err := json.Marshal(pipeline)
	if err != nil {
		log.Printf("[webhook] failed to marshal pipeline spec: %v", err)
		http.Error(w, "failed to marshal pipeline specification", http.StatusInternalServerError)
		return
	}

	if err := h.store.CreateRunWithDelivery(r.Context(), delivery, run, nil, string(pipelineSpecBytes)); err != nil {
		if errors.Is(err, store.ErrDuplicateDelivery) {
			log.Printf("[webhook] duplicate delivery %s for %s event detected", deliveryID, event)
			existing, err := h.store.GetDelivery(r.Context(), deliveryID)
			if err != nil || existing == nil {
				log.Printf("[webhook] duplicate delivery %s lookup failed: %v", deliveryID, err)
				http.Error(w, "failed to query existing delivery", http.StatusInternalServerError)
				return
			}

			// Verify referenced run actually exists
			existingRun, err := h.store.GetRun(r.Context(), existing.RunID)
			if err != nil || existingRun == nil {
				log.Printf("[webhook] duplicate delivery %s references non-existent run %s: %v", deliveryID, existing.RunID, err)
				http.Error(w, "inconsistent webhook delivery state: run not found", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":      "duplicate",
				"message":     "delivery already processed",
				"delivery_id": deliveryID,
				"run_id":      existing.RunID,
			})
			return
		}
		log.Printf("[webhook] failed to record run and delivery %s: %v", deliveryID, err)
		http.Error(w, "failed to record run and delivery", http.StatusInternalServerError)
		return
	}

	logW, err := h.streamer.Writer(runID)
	if err != nil {
		log.Printf("[webhook] failed to create log writer for run %s: %v", runID, err)
		if failErr := h.store.FailRunWithDelivery(context.Background(), runID, nil, deliveryID); failErr != nil {
			log.Printf("[webhook] failed atomic cleanup for run %s after streamer error: %v", runID, failErr)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.queue.Enqueue(&queue.Job{
		RunID:      runID,
		Pipeline:   pipeline,
		LogWriter:  logW,
		EnqueuedAt: time.Now(),
	}); err != nil {
		log.Printf("[webhook] run %s rejected: %v", runID, err)
		fmt.Fprintf(logW, "[drev] pipeline rejected: %v\n", err)
		logW.Close()
		if failErr := h.store.FailRunWithDelivery(context.Background(), runID, nil, deliveryID); failErr != nil {
			log.Printf("[webhook] failed atomic cleanup for run %s after queue rejection: %v", runID, failErr)
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	shortSHA := commitSHA
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	resp := map[string]interface{}{
		"run_id":       runID,
		"repo":         repoFullName,
		"branch":       branch,
		"commit":       shortSHA,
		"triggered_by": triggeredBy,
		"event":        event,
		"delivery_id":  deliveryID,
	}
	if prNumber > 0 {
		resp["pr_number"] = prNumber
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}
