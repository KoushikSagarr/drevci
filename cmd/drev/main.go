package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var (
	serverURL string
	token     string
)

var client = &http.Client{
	Timeout: 30 * time.Second,
}

func doReq(req *http.Request) (*http.Response, error) {
	if token == "" {
		token = os.Getenv("DREV_TOKEN")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("HTTP error %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	return res, nil
}

func main() {
	_ = godotenv.Load()

	var rootCmd = &cobra.Command{
		Use:     "drev",
		Version: "0.1.0",
	}
	rootCmd.SetVersionTemplate("Drev CI v{{.Version}}\n")
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9090", "server URL")
	rootCmd.PersistentFlags().StringVar(&token, "token", "", "API token (or read from DREV_TOKEN env var)")

	if envSrv := os.Getenv("DREV_SERVER"); envSrv != "" {
		serverURL = envSrv
	}

	var follow bool
	var envOverrides []string
	var timeout time.Duration
	var pollInterval time.Duration

	var runCmd = &cobra.Command{
		Use:   "run <pipeline-file>",
		Short: "Trigger a pipeline run",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			path := args[0]
			effectiveTimeout := timeout
			if !cmd.Flags().Changed("timeout") {
				if envTimeout := os.Getenv("DREV_TIMEOUT"); envTimeout != "" {
					if d, err := time.ParseDuration(envTimeout); err == nil {
						effectiveTimeout = d
					}
				}
			}

			exitCode := ExecuteRun(cmd.Context(), RunConfig{
				ServerURL:    serverURL,
				Token:        token,
				PipelinePath: path,
				EnvOverrides: envOverrides,
				Follow:       follow,
				Timeout:      effectiveTimeout,
				PollInterval: pollInterval,
				Client:       client,
				Out:          os.Stdout,
				ErrOut:       os.Stderr,
			})
			if exitCode != 0 {
				os.Exit(exitCode)
			}
		},
	}
	runCmd.Flags().BoolVar(&follow, "follow", true, "stream logs after triggering")
	runCmd.Flags().StringSliceVar(&envOverrides, "env", nil, "extra env overrides KEY=VALUE")
	runCmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "maximum time to wait for pipeline completion (0 for unlimited)")
	runCmd.Flags().DurationVar(&pollInterval, "poll-interval", 1*time.Second, "polling interval for status checks")

	var statusCmd = &cobra.Command{
		Use:   "status <run-id>",
		Short: "Check run status",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runID := args[0]

			req, _ := http.NewRequest("GET", serverURL+"/api/v1/runs/"+runID, nil)
			res, err := doReq(req)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer res.Body.Close()

			var run drevtypes.Run
			json.NewDecoder(res.Body).Decode(&run)

			req, _ = http.NewRequest("GET", serverURL+"/api/v1/runs/"+runID+"/jobs", nil)
			res2, err := doReq(req)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer res2.Body.Close()

			var jobs []drevtypes.RunJob
			json.NewDecoder(res2.Body).Decode(&jobs)

			fmt.Printf("Run ID:   %s\n", run.ID)
			fmt.Printf("Status:   %s\n", run.Status)
			started := ""
			if !run.StartedAt.IsZero() {
				started = run.StartedAt.Format(time.RFC3339)
			}
			fmt.Printf("Started:  %s\n", started)
			finished := ""
			if !run.FinishedAt.IsZero() {
				finished = run.FinishedAt.Format(time.RFC3339)
			}
			fmt.Printf("Finished: %s\n\n", finished)

			fmt.Println("Jobs:")
			for _, j := range jobs {
				mark := " "
				if j.Status == drevtypes.StatusSuccess {
					mark = "✓"
				} else if j.Status == drevtypes.StatusFailed {
					mark = "✗"
				} else if j.Status == drevtypes.StatusRunning {
					mark = "↻"
				}
				dur := ""
				if !j.StartedAt.IsZero() && !j.FinishedAt.IsZero() {
					dur = j.FinishedAt.Sub(j.StartedAt).Round(time.Millisecond).String()
				}
				fmt.Printf("  %s %-10s (%s)\n", mark, j.JobName, dur)
			}
		},
	}

	var logsCmd = &cobra.Command{
		Use:   "logs <run-id>",
		Short: "Stream or print logs for a run",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runID := args[0]
			if err := streamLogs(cmd.Context(), client, serverURL, token, runID, os.Stdout, os.Stderr); err != nil {
				os.Exit(1)
			}
		},
	}
	logsCmd.Flags().BoolVar(&follow, "follow", false, "stream logs")

	var tokenCmd = &cobra.Command{
		Use:   "token",
		Short: "Token operations",
	}
	var tokenGenCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate a new API token",
		Run: func(cmd *cobra.Command, args []string) {
			t, err := auth.GenerateToken()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(t)
			fmt.Println("\nAdd to your environment: export DREV_TOKEN=" + t)
			fmt.Println("Start server with: drevd --token " + t)
		},
	}
	tokenCmd.AddCommand(tokenGenCmd)

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Drev CI v0.1.0")
		},
	}

	rootCmd.AddCommand(runCmd, statusCmd, logsCmd, tokenCmd, versionCmd)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
