package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/drevci/drev/internal/api"
	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/internal/notify"
	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/pool"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/runner"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	_ = godotenv.Load()

	var port int
	var dbPath string
	var logDir string
	var token string
	var host string
	var webhookSecret string
	var webhookConfig string
	var workers int
	var queueSize int
	var slackWebhook string
	var discordWebhook string
	var dbType string
	var dbURL string
	var dockerHost string

	var rootCmd = &cobra.Command{
		Use:     "drevd",
		Version: "0.1.0",
		Short:   "Drev CI server daemon",
		Run: func(cmd *cobra.Command, args []string) {
			if dockerHost == "" {
				dockerHost = os.Getenv("DOCKER_HOST")
			}
			if dockerHost != "" {
				os.Setenv("DOCKER_HOST", dockerHost)
			}

			if token == "" {
				token = os.Getenv("DREV_TOKEN")
			}

			envUpdates := make(map[string]string)
			if token == "" {
				t, err := auth.GenerateToken()
				if err != nil {
					log.Fatalf("failed to generate token: %v", err)
				}
				token = t
				fmt.Println("No API token provided; generated a new token and saved to .env.")
				envUpdates["DREV_TOKEN"] = token
			}

			if webhookSecret == "" {
				webhookSecret = os.Getenv("DREV_WEBHOOK_SECRET")
			}
			if webhookSecret == "" {
				t, err := auth.GenerateToken()
				if err != nil {
					log.Fatalf("failed to generate webhook secret: %v", err)
				}
				webhookSecret = t
				fmt.Println("No webhook secret provided; generated a new secret and saved to .env.")
				envUpdates["DREV_WEBHOOK_SECRET"] = webhookSecret
			}

			if len(envUpdates) > 0 {
				if err := auth.PersistEnvSecrets(".env", envUpdates); err != nil {
					log.Printf("Warning: failed to persist generated secrets to .env: %v", err)
				} else {
					fmt.Println("Saved newly generated secrets to .env file for persistence.")
				}
			}

			// Clamp workers
			if workers < 1 {
				workers = 1
			}
			if workers > 20 {
				workers = 20
			}

			// Expose token to API via env var
			os.Setenv("DREV_API_TOKENS", token)

			if err := os.MkdirAll(logDir, 0755); err != nil {
				log.Fatalf("failed to create log dir: %v", err)
			}

			// Resolve database type and URL from flags/env
			if dbType == "" {
				dbType = os.Getenv("DREV_DB_TYPE")
			}
			if dbType == "" {
				dbType = "sqlite"
			}
			if dbURL == "" {
				dbURL = os.Getenv("DREV_DB_URL")
			}

			var s store.Store
			switch dbType {
			case "postgres":
				if dbURL == "" {
					log.Fatal("--db-url or DREV_DB_URL is required when --db-type=postgres")
				}
				pg, err := store.OpenPostgres(dbURL)
				if err != nil {
					log.Fatalf("failed to open postgres store: %v", err)
				}
				defer pg.Close()
				s = pg
				fmt.Println("Database: PostgreSQL (Supabase)")
			default:
				sg, err := store.Open(dbPath)
				if err != nil {
					log.Fatalf("failed to open sqlite store: %v", err)
				}
				defer sg.Close()
				s = sg
				fmt.Printf("Database: SQLite (%s)\n", dbPath)
			}

			// 1. Recover interrupted runs in database from previous session
			interruptedRunIDs, err := s.ResetGhostRuns(context.Background())
			if err != nil {
				log.Printf("Warning: failed to reset ghost runs: %v", err)
			} else if len(interruptedRunIDs) > 0 {
				log.Printf("Recovered %d interrupted pipeline run(s) from previous session: %v", len(interruptedRunIDs), interruptedRunIDs)
			}

			// 2. Initialize runner
			r, err := runner.New(s)
			if err != nil {
				log.Fatalf("failed to create runner: %v", err)
			}

			// 3. Validate Docker daemon connectivity and reconcile orphaned containers
			if cli, err := runner.NewDockerClient(); err == nil {
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
				if _, err := cli.Ping(pingCtx); err != nil {
					log.Printf("Warning: Docker daemon is unreachable: %v. Pipelines requiring container execution will fail until Docker is running.", err)
					if os.Getenv("DOCKER_HOST") == "" && runtime.GOOS == "darwin" {
						log.Println("Hint: If using Docker Desktop on macOS, ensure it is running or set DOCKER_HOST=\"unix://$HOME/.docker/run/docker.sock\".")
					}
				} else {
					// Docker daemon reachable: safely reconcile leftover/orphaned Drev CI containers
					recCtx, recCancel := context.WithTimeout(context.Background(), 30*time.Second)
					summary, recErr := r.ReconcileOrphans(recCtx, interruptedRunIDs)
					recCancel()
					runner.LogReconciliationSummary(log.Printf, summary, recErr)
				}
				pingCancel()
				cli.Close()
			}

			sched := scheduler.New(r, s)
			p := parser.NewParser()
			stream := streamer.New(logDir)

			// Notifications
			if slackWebhook == "" {
				slackWebhook = os.Getenv("DREV_SLACK_WEBHOOK")
			}
			if discordWebhook == "" {
				discordWebhook = os.Getenv("DREV_DISCORD_WEBHOOK")
			}
			notifEnabled := slackWebhook != "" || discordWebhook != ""
			notif := notify.New(notify.NotificationConfig{
				SlackWebhookURL:   slackWebhook,
				DiscordWebhookURL: discordWebhook,
				Enabled:           notifEnabled,
			})
			if notifEnabled {
				slackStatus := "no"
				if slackWebhook != "" {
					slackStatus = "yes"
				}
				discordStatus := "no"
				if discordWebhook != "" {
					discordStatus = "yes"
				}
				fmt.Printf("Notifications enabled: Slack=%s Discord=%s\n", slackStatus, discordStatus)
			} else {
				fmt.Println("Notifications disabled (no webhooks configured)")
			}

			q := queue.New(queueSize)
			wp := pool.New(workers, q, sched, s, notif)

			ctx, cancel := context.WithCancel(context.Background())
			var stopPoolOnce sync.Once
			stopPool := func() {
				stopPoolOnce.Do(func() {
					cancel()
					wp.Stop()
				})
			}
			defer stopPool()

			wp.Start(ctx)

			wh := webhook.New(s, q, p, stream, webhookSecret, webhookConfig)
			h := api.New(s, sched, p, stream, q, workers, wh, logDir, notifEnabled)

			addr := fmt.Sprintf("%s:%d", host, port)
			server := &http.Server{
				Addr:    addr,
				Handler: h.Routes(),
			}

			fmt.Println("┌──────────────────────────────┐")
			fmt.Printf("│   Drev CI  v0.1.0           │\n")
			fmt.Printf("│   http://%-17s │\n", addr)
			fmt.Printf("│   Workers: %-17d │\n", workers)
			fmt.Println("└──────────────────────────────┘")
			fmt.Printf("Webhook URL: http://%s/webhooks/github\n", addr)

			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("server error: %v", err)
				}
			}()

			stop := make(chan os.Signal, 1)
			signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
			<-stop

			fmt.Println("\nShutting down gracefully...")

			// 1. First drain/stop HTTP server so no new incoming requests or webhook jobs can be enqueued.
			shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutCancel()

			if err := server.Shutdown(shutCtx); err != nil {
				log.Printf("HTTP server shutdown warning: %v", err)
			}

			// 2. Stop workers: cancel context to signal workers to terminate, and wait for pool to exit.
			stopPool()
		},
	}

	rootCmd.Flags().IntVar(&port, "port", 9090, "HTTP port")
	rootCmd.Flags().StringVar(&dbPath, "db", "./drev.db", "SQLite DB path")
	rootCmd.Flags().StringVar(&logDir, "log-dir", "./logs", "log file directory")
	rootCmd.Flags().StringVar(&token, "token", "", "API token (or DREV_TOKEN env var)")
	rootCmd.Flags().StringVar(&host, "host", "0.0.0.0", "bind host")
	rootCmd.Flags().StringVar(&webhookSecret, "webhook-secret", "", "HMAC secret for GitHub (or DREV_WEBHOOK_SECRET env)")
	rootCmd.Flags().StringVar(&webhookConfig, "webhook-config", "./configs/webhooks", "dir for per-repo pipeline configs")
	rootCmd.Flags().IntVar(&workers, "workers", 3, "number of concurrent pipeline workers (1-20)")
	rootCmd.Flags().IntVar(&queueSize, "queue-size", 100, "max queued pipelines")
	rootCmd.Flags().StringVar(&slackWebhook, "slack-webhook", "", "Slack incoming webhook URL (or DREV_SLACK_WEBHOOK env)")
	rootCmd.Flags().StringVar(&discordWebhook, "discord-webhook", "", "Discord webhook URL (or DREV_DISCORD_WEBHOOK env)")
	rootCmd.Flags().StringVar(&dbType, "db-type", "", "Database type: sqlite (default) or postgres (or DREV_DB_TYPE env)")
	rootCmd.Flags().StringVar(&dbURL, "db-url", "", "PostgreSQL connection string (or DREV_DB_URL env)")
	rootCmd.Flags().StringVar(&dockerHost, "docker-host", "", "Docker daemon host/socket endpoint (or DOCKER_HOST env)")

	rootCmd.SetVersionTemplate("Drev CI v{{.Version}}\n")

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Drev CI v0.1.0")
		},
	}
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
