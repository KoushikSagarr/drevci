package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/drevci/drev/internal/api"
	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/runner"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/streamer"
	"github.com/drevci/drev/internal/webhook"
	"github.com/joho/godotenv"
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

	var rootCmd = &cobra.Command{
		Use:   "drevd",
		Short: "Drev CI server daemon",
		Run: func(cmd *cobra.Command, args []string) {
			if token == "" {
				token = os.Getenv("DREV_TOKEN")
			}

			var envUpdates []string
			if token == "" {
				t, err := auth.GenerateToken()
				if err != nil {
					log.Fatalf("failed to generate token: %v", err)
				}
				token = t
				fmt.Printf("No token set. Generated token: %s\n", token)
				envUpdates = append(envUpdates, fmt.Sprintf("DREV_TOKEN=%s", token))
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
				fmt.Printf("No webhook secret set. Generated secret: %s\n", webhookSecret)
				envUpdates = append(envUpdates, fmt.Sprintf("DREV_WEBHOOK_SECRET=%s", webhookSecret))
			}

			if len(envUpdates) > 0 {
				f, err := os.OpenFile(".env", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
				if err == nil {
					for _, update := range envUpdates {
						f.WriteString(update + "\n")
					}
					f.Close()
					fmt.Println("Saved newly generated secrets to .env file for persistence.")
				}
			}

			// Expose token to API via env var
			os.Setenv("DREV_API_TOKENS", token)

			if err := os.MkdirAll(logDir, 0755); err != nil {
				log.Fatalf("failed to create log dir: %v", err)
			}

			s, err := store.Open(dbPath)
			if err != nil {
				log.Fatalf("failed to open store: %v", err)
			}
			defer s.Close()

			r, err := runner.New(s)
			if err != nil {
				log.Fatalf("failed to create runner: %v", err)
			}

			sched := scheduler.New(r, s)
			p := parser.NewParser()
			stream := streamer.New(logDir)
			wh := webhook.New(s, sched, p, stream, webhookSecret, webhookConfig)

			h := api.New(s, sched, p, stream, wh, logDir)

			addr := fmt.Sprintf("%s:%d", host, port)
			server := &http.Server{
				Addr:    addr,
				Handler: h.Routes(),
			}

			fmt.Println("┌─────────────────────────┐")
			fmt.Printf("│   Drev CI  v0.1.0      │\n")
			fmt.Printf("│   http://%-13s │\n", addr)
			fmt.Println("└─────────────────────────┘")
			fmt.Printf("Webhook URL: http://%s/webhooks/github\n", addr)
			fmt.Printf("Webhook secret: %s\n", webhookSecret)

			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("server error: %v", err)
				}
			}()

			stop := make(chan os.Signal, 1)
			signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
			<-stop

			fmt.Println("\nShutting down gracefully...")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := server.Shutdown(ctx); err != nil {
				log.Fatalf("shutdown error: %v", err)
			}
		},
	}

	rootCmd.Flags().IntVar(&port, "port", 9090, "HTTP port")
	rootCmd.Flags().StringVar(&dbPath, "db", "./drev.db", "SQLite DB path")
	rootCmd.Flags().StringVar(&logDir, "log-dir", "./logs", "log file directory")
	rootCmd.Flags().StringVar(&token, "token", "", "API token (or DREV_TOKEN env var)")
	rootCmd.Flags().StringVar(&host, "host", "0.0.0.0", "bind host")
	rootCmd.Flags().StringVar(&webhookSecret, "webhook-secret", "", "HMAC secret for GitHub (or DREV_WEBHOOK_SECRET env)")
	rootCmd.Flags().StringVar(&webhookConfig, "webhook-config", "./configs/webhooks", "dir for per-repo pipeline configs")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
