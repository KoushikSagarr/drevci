package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// PollOptions configures the status polling loop.
type PollOptions struct {
	Client       *http.Client
	ServerURL    string
	Token        string
	RunID        string
	PollInterval time.Duration
	Timeout      time.Duration
	Out          io.Writer
	ErrOut       io.Writer
}

// PollRunStatus polls the run status until a terminal state is reached,
// or until the context times out / is cancelled.
//
// Exit Codes:
//
//	0: Pipeline succeeded (StatusSuccess)
//	1: Pipeline failed (StatusFailed), cancelled (StatusCancelled), unexpected/unknown status,
//	   polling/API error, timeout, or context cancellation.
func PollRunStatus(ctx context.Context, opts PollOptions) (int, error) {
	if opts.Client == nil {
		opts.Client = client
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 1 * time.Second
	}
	if opts.Out == nil {
		opts.Out = os.Stdout
	}
	if opts.ErrOut == nil {
		opts.ErrOut = os.Stderr
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	for {
		status, err := fetchRunStatus(ctx, opts.Client, opts.ServerURL, opts.Token, opts.RunID)
		if err != nil {
			if ctx.Err() != nil {
				return handleContextError(ctx, opts.ErrOut, opts.Timeout)
			}
			fmt.Fprintf(opts.ErrOut, "Error checking status: %v\n", err)
			return 1, err
		}

		switch status {
		case drevtypes.StatusSuccess:
			fmt.Fprintln(opts.Out, "✓ Pipeline succeeded")
			return 0, nil

		case drevtypes.StatusFailed:
			fmt.Fprintf(opts.ErrOut, "✗ Pipeline failed (status: %s)\n", status)
			return 1, nil

		case drevtypes.StatusCancelled:
			fmt.Fprintf(opts.ErrOut, "✗ Pipeline cancelled (status: %s)\n", status)
			return 1, nil

		case drevtypes.StatusPending, drevtypes.StatusRunning:
			// Nonterminal states: continue polling. NEVER report success.
			select {
			case <-ctx.Done():
				return handleContextError(ctx, opts.ErrOut, opts.Timeout)
			case <-ticker.C:
				continue
			}

		default:
			// Unknown, empty, or malformed status: fail clearly rather than assuming success.
			fmt.Fprintf(opts.ErrOut, "✗ Pipeline completed with unexpected status: %q\n", status)
			return 1, fmt.Errorf("unexpected pipeline status: %q", status)
		}
	}
}

func fetchRunStatus(ctx context.Context, httpClient *http.Client, serverURL, token, runID string) (drevtypes.RunStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/api/v1/runs/"+runID, nil)
	if err != nil {
		return "", fmt.Errorf("creating status request: %w", err)
	}
	if token == "" {
		token = os.Getenv("DREV_TOKEN")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting run status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP error %d checking run status: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var run drevtypes.Run
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		return "", fmt.Errorf("decoding run status response: %w", err)
	}

	return run.Status, nil
}

func handleContextError(ctx context.Context, errOut io.Writer, timeout time.Duration) (int, error) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		fmt.Fprintf(errOut, "✗ Pipeline timed out waiting for completion (timeout: %v)\n", timeout)
		return 1, ctx.Err()
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		fmt.Fprintln(errOut, "✗ Pipeline wait interrupted / cancelled")
		return 1, ctx.Err()
	}
	fmt.Fprintf(errOut, "✗ Pipeline wait aborted: %v\n", ctx.Err())
	return 1, ctx.Err()
}

// RunConfig holds configuration for executing the `drev run` command.
type RunConfig struct {
	ServerURL    string
	Token        string
	PipelinePath string
	EnvOverrides []string
	Follow       bool
	Timeout      time.Duration
	PollInterval time.Duration
	Client       *http.Client
	Out          io.Writer
	ErrOut       io.Writer
}

// ExecuteRun executes the trigger, log streaming, and terminal status polling for a pipeline.
// Returns an exit code: 0 on successful pipeline completion, 1 on failure, cancellation, timeout, or error.
func ExecuteRun(ctx context.Context, cfg RunConfig) int {
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	if cfg.ErrOut == nil {
		cfg.ErrOut = os.Stderr
	}
	if cfg.Client == nil {
		cfg.Client = client
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}

	envMap := make(map[string]string)
	for _, e := range cfg.EnvOverrides {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	payload := map[string]interface{}{
		"pipeline_path": cfg.PipelinePath,
		"env":           envMap,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(cfg.ErrOut, "Error encoding trigger payload: %v\n", err)
		return 1
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.ServerURL+"/api/v1/pipelines/trigger", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(cfg.ErrOut, "Error creating trigger request: %v\n", err)
		return 1
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Token == "" {
		cfg.Token = os.Getenv("DREV_TOKEN")
	}
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	res, err := cfg.Client.Do(req)
	if err != nil {
		fmt.Fprintf(cfg.ErrOut, "Error: %v\n", err)
		return 1
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		resBody, _ := io.ReadAll(res.Body)
		fmt.Fprintf(cfg.ErrOut, "HTTP Error %d: %s\n", res.StatusCode, strings.TrimSpace(string(resBody)))
		return 1
	}

	var resp map[string]string
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		fmt.Fprintf(cfg.ErrOut, "Error decoding trigger response: %v\n", err)
		return 1
	}
	runID := resp["run_id"]
	if runID == "" {
		fmt.Fprintln(cfg.ErrOut, "Error: server did not return a run ID")
		return 1
	}

	fmt.Fprintf(cfg.Out, "▶ Run started: %s\n", runID)

	if cfg.Follow {
		_ = streamLogs(ctx, cfg.Client, cfg.ServerURL, cfg.Token, runID, cfg.Out, cfg.ErrOut)
	}

	exitCode, _ := PollRunStatus(ctx, PollOptions{
		Client:       cfg.Client,
		ServerURL:    cfg.ServerURL,
		Token:        cfg.Token,
		RunID:        runID,
		PollInterval: cfg.PollInterval,
		Timeout:      cfg.Timeout,
		Out:          cfg.Out,
		ErrOut:       cfg.ErrOut,
	})

	return exitCode
}

func streamLogs(ctx context.Context, httpClient *http.Client, serverURL, token, runID string, out, errOut io.Writer) error {
	if token == "" {
		token = os.Getenv("DREV_TOKEN")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/api/v1/runs/"+runID+"/logs", nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "text/event-stream")

	streamClient := &http.Client{} // no timeout for streams
	res, err := streamClient.Do(req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Fprintf(errOut, "Error streaming logs: %v\n", err)
		}
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		body, _ := io.ReadAll(res.Body)
		fmt.Fprintf(errOut, "HTTP Error %d streaming logs: %s\n", res.StatusCode, strings.TrimSpace(string(body)))
		return fmt.Errorf("HTTP Error %d streaming logs: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}

	reader := bufio.NewReader(res.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(errOut, "\nStream error: %v\n", err)
			}
			break
		}
		if strings.HasPrefix(line, "data: ") {
			fmt.Fprint(out, strings.TrimPrefix(line, "data: "))
		}
	}
	return nil
}
