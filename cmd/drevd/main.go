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
)

func main() {
	var port int
	var dbPath string
	var logDir string
	var token string
	var host string

	var rootCmd = &cobra.Command{
		Use:   "drevd",
		Short: "Drev CI server daemon",
		Run: func(cmd *cobra.Command, args []string) {
			if token == "" {
				token = os.Getenv("DREV_TOKEN")
			}
			if token == "" {
				t, err := auth.GenerateToken()
				if err != nil {
					log.Fatalf("failed to generate token: %v", err)
				}
				token = t
				fmt.Printf("No token set. Generated token: %s\n", token)
				fmt.Printf("Set DREV_TOKEN=%s to reuse on restart\n", token)
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

			h := api.New(s, sched, p, logDir)

			addr := fmt.Sprintf("%s:%d", host, port)
			server := &http.Server{
				Addr:    addr,
				Handler: h.Routes(),
			}

			fmt.Println("┌─────────────────────────┐")
			fmt.Printf("│   Drev CI  v0.1.0      │\n")
			fmt.Printf("│   http://%-13s │\n", addr)
			fmt.Println("└─────────────────────────┘")

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

	rootCmd.Flags().IntVar(&port, "port", 8080, "HTTP port")
	rootCmd.Flags().StringVar(&dbPath, "db", "./drev.db", "SQLite DB path")
	rootCmd.Flags().StringVar(&logDir, "log-dir", "./logs", "log file directory")
	rootCmd.Flags().StringVar(&token, "token", "", "API token (or DREV_TOKEN env var)")
	rootCmd.Flags().StringVar(&host, "host", "0.0.0.0", "bind host")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
