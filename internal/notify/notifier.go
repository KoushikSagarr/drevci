package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"golang.org/x/sync/errgroup"
)

// NotificationConfig holds webhook URLs for external notification services.
type NotificationConfig struct {
	SlackWebhookURL   string // optional
	DiscordWebhookURL string // optional
	DashboardURL      string // base URL for "View in Dashboard" links
	Enabled           bool
}

// Notifier sends pipeline completion notifications to external services.
type Notifier struct {
	config NotificationConfig
	client *http.Client
}

// New creates a new Notifier. If no webhook URLs are configured, notifications
// are silently skipped when NotifyPipelineComplete is called.
func New(cfg NotificationConfig) *Notifier {
	if cfg.DashboardURL == "" {
		cfg.DashboardURL = "http://localhost:3000"
	}
	return &Notifier{
		config: cfg,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// NotifyPipelineComplete sends a notification to all configured webhook
// endpoints when a pipeline run finishes (success or failed).
func (n *Notifier) NotifyPipelineComplete(
	ctx context.Context,
	run *drevtypes.Run,
	pipeline *drevtypes.Pipeline,
	jobs []*drevtypes.RunJob,
) error {
	if !n.config.Enabled {
		return nil
	}

	eg, egCtx := errgroup.WithContext(ctx)

	if n.config.SlackWebhookURL != "" {
		eg.Go(func() error {
			if err := n.sendSlack(egCtx, run, pipeline, jobs); err != nil {
				log.Printf("[notify] slack error: %v", err)
				return err
			}
			return nil
		})
	}

	if n.config.DiscordWebhookURL != "" {
		eg.Go(func() error {
			if err := n.sendDiscord(egCtx, run, pipeline, jobs); err != nil {
				log.Printf("[notify] discord error: %v", err)
				return err
			}
			return nil
		})
	}

	return eg.Wait()
}

// --- Slack ---

func (n *Notifier) sendSlack(
	ctx context.Context,
	run *drevtypes.Run,
	pipeline *drevtypes.Pipeline,
	jobs []*drevtypes.RunJob,
) error {
	statusText := ":white_check_mark: *Succeeded*"
	color := "#36a64f"
	if run.Status == drevtypes.StatusFailed {
		statusText = ":x: *Failed*"
		color = "#e74c3c"
	}

	shortID := run.ID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	duration := formatDuration(run.StartedAt, run.FinishedAt)
	started := run.StartedAt.Format("Jan 2, 15:04:05")

	// Build job summary lines
	jobSummary := ""
	for _, j := range jobs {
		mark := "✓"
		if j.Status == drevtypes.StatusFailed {
			mark = "✗"
		} else if j.Status == drevtypes.StatusRunning {
			mark = "↻"
		} else if j.Status == drevtypes.StatusPending {
			mark = "○"
		}
		dur := formatDuration(j.StartedAt, j.FinishedAt)
		jobSummary += fmt.Sprintf("%s %s (%s)\n", mark, j.JobName, dur)
	}

	dashboardURL := fmt.Sprintf("%s/runs/%s", n.config.DashboardURL, run.ID)

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color": color,
				"blocks": []map[string]interface{}{
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": fmt.Sprintf("*Pipeline: %s*\nStatus: %s", pipeline.Name, statusText),
						},
					},
					{
						"type": "section",
						"fields": []map[string]string{
							{"type": "mrkdwn", "text": fmt.Sprintf("*Run ID*\n`%s`", shortID)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Duration*\n%s", duration)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Started*\n%s", started)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Jobs*\n%d", len(jobs))},
						},
					},
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": fmt.Sprintf("*Jobs Summary*\n```%s```", jobSummary),
						},
					},
					{
						"type": "actions",
						"elements": []map[string]interface{}{
							{
								"type": "button",
								"text": map[string]string{
									"type": "plain_text",
									"text": "View in Dashboard",
								},
								"url": dashboardURL,
							},
						},
					},
				},
			},
		},
	}

	return n.postJSON(ctx, n.config.SlackWebhookURL, payload)
}

// --- Discord ---

func (n *Notifier) sendDiscord(
	ctx context.Context,
	run *drevtypes.Run,
	pipeline *drevtypes.Pipeline,
	jobs []*drevtypes.RunJob,
) error {
	statusDesc := "✓ Pipeline Succeeded"
	statusValue := "Success"
	color := 3394351 // green #33CC4F
	if run.Status == drevtypes.StatusFailed {
		statusDesc = "✗ Pipeline Failed"
		statusValue = "Failed"
		color = 15158332 // red #E74C3C
	}

	shortID := run.ID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	duration := formatDuration(run.StartedAt, run.FinishedAt)

	// Build job lines for description
	jobLines := ""
	for _, j := range jobs {
		mark := "✓"
		if j.Status == drevtypes.StatusFailed {
			mark = "✗"
		} else if j.Status == drevtypes.StatusRunning {
			mark = "↻"
		} else if j.Status == drevtypes.StatusPending {
			mark = "○"
		}
		dur := formatDuration(j.StartedAt, j.FinishedAt)
		jobLines += fmt.Sprintf("%s **%s** (%s)\n", mark, j.JobName, dur)
	}

	dashboardURL := fmt.Sprintf("%s/runs/%s", n.config.DashboardURL, run.ID)

	payload := map[string]interface{}{
		"embeds": []map[string]interface{}{
			{
				"title":       fmt.Sprintf("Pipeline: %s", pipeline.Name),
				"description": fmt.Sprintf("%s\n\n%s", statusDesc, jobLines),
				"color":       color,
				"fields": []map[string]interface{}{
					{"name": "Run ID", "value": fmt.Sprintf("`%s`", shortID), "inline": true},
					{"name": "Duration", "value": duration, "inline": true},
					{"name": "Status", "value": statusValue, "inline": true},
					{"name": "Jobs", "value": fmt.Sprintf("%d", len(jobs)), "inline": true},
				},
				"timestamp": run.FinishedAt.Format(time.RFC3339),
			},
		},
		"components": []map[string]interface{}{
			{
				"type": 1,
				"components": []map[string]interface{}{
					{
						"type":  2,
						"label": "View Run",
						"url":   dashboardURL,
						"style": 5,
					},
				},
			},
		},
	}

	return n.postJSON(ctx, n.config.DiscordWebhookURL, payload)
}

// --- Helpers ---

func (n *Notifier) postJSON(ctx context.Context, url string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook %s returned status %d", url, resp.StatusCode)
	}

	return nil
}

func formatDuration(start, end time.Time) string {
	if start.IsZero() {
		return "-"
	}
	finish := end
	if finish.IsZero() {
		finish = time.Now()
	}
	diff := int(finish.Sub(start).Seconds())
	if diff < 0 {
		diff = 0
	}
	if diff < 60 {
		return fmt.Sprintf("%ds", diff)
	}
	m := diff / 60
	s := diff % 60
	if m < 60 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := m / 60
	return fmt.Sprintf("%dh %dm", h, m%60)
}
