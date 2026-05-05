package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
		fmt.Printf("HTTP Error %d: %s\n", res.StatusCode, string(body))
		os.Exit(1)
	}
	return res, nil
}

func main() {
	_ = godotenv.Load()

	var rootCmd = &cobra.Command{Use: "drev"}
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9090", "server URL")
	rootCmd.PersistentFlags().StringVar(&token, "token", "", "API token (or read from DREV_TOKEN env var)")

	if envSrv := os.Getenv("DREV_SERVER"); envSrv != "" {
		serverURL = envSrv
	}

	var follow bool
	var envOverrides []string

	var runCmd = &cobra.Command{
		Use:   "run <pipeline-file>",
		Short: "Trigger a pipeline run",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			path := args[0]
			envMap := make(map[string]string)
			for _, e := range envOverrides {
				parts := strings.SplitN(e, "=", 2)
				if len(parts) == 2 {
					envMap[parts[0]] = parts[1]
				}
			}

			payload := map[string]interface{}{
				"pipeline_path": path,
				"env":           envMap,
			}
			body, _ := json.Marshal(payload)

			req, _ := http.NewRequest("POST", serverURL+"/api/v1/pipelines/trigger", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			
			res, err := doReq(req)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer res.Body.Close()

			var resp map[string]string
			json.NewDecoder(res.Body).Decode(&resp)
			runID := resp["run_id"]

			fmt.Printf("▶ Run started: %s\n", runID)

			if follow {
				streamLogs(runID)
			}

			req, _ = http.NewRequest("GET", serverURL+"/api/v1/runs/"+runID, nil)
			res, err = doReq(req)
			if err != nil {
				fmt.Printf("Error checking status: %v\n", err)
				os.Exit(1)
			}
			defer res.Body.Close()

			var run drevtypes.Run
			json.NewDecoder(res.Body).Decode(&run)

			if run.Status == drevtypes.StatusFailed || run.Status == drevtypes.StatusCancelled {
				fmt.Printf("✗ Pipeline failed (status: %s)\n", run.Status)
				os.Exit(1)
			} else {
				fmt.Println("✓ Pipeline succeeded")
				os.Exit(0)
			}
		},
	}
	runCmd.Flags().BoolVar(&follow, "follow", true, "stream logs after triggering")
	runCmd.Flags().StringSliceVar(&envOverrides, "env", nil, "extra env overrides KEY=VALUE")

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
			streamLogs(runID)
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

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func streamLogs(runID string) {
	if token == "" {
		token = os.Getenv("DREV_TOKEN")
	}

	req, _ := http.NewRequest("GET", serverURL+"/api/v1/runs/"+runID+"/logs", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "text/event-stream")

	streamClient := &http.Client{} // no timeout for streams
	res, err := streamClient.Do(req)
	if err != nil {
		fmt.Printf("Error streaming logs: %v\n", err)
		return
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		body, _ := io.ReadAll(res.Body)
		fmt.Printf("HTTP Error %d: %s\n", res.StatusCode, string(body))
		os.Exit(1)
	}

	reader := bufio.NewReader(res.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Printf("\nStream error: %v\n", err)
			}
			break
		}
		if strings.HasPrefix(line, "data: ") {
			fmt.Print(strings.TrimPrefix(line, "data: "))
		}
	}
}
