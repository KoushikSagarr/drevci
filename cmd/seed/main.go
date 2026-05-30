package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/drevci/drev/internal/auth"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	var dbType string
	var dbURL string
	var dbPath string

	flag.StringVar(&dbType, "db-type", "", "Database type: sqlite or postgres (or DREV_DB_TYPE env)")
	flag.StringVar(&dbURL, "db-url", "", "PostgreSQL connection string (or DREV_DB_URL env)")
	flag.StringVar(&dbPath, "db", "./drev.db", "SQLite DB path")
	flag.Parse()

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
		fmt.Println("Seeding PostgreSQL (Supabase)...")
	default:
		sg, err := store.Open(dbPath)
		if err != nil {
			log.Fatalf("failed to open sqlite store: %v", err)
		}
		defer sg.Close()
		s = sg
		fmt.Printf("Seeding SQLite (%s)...\n", dbPath)
	}

	ctx := context.Background()

	// 1. If SaaS mode, create demo organization and API token
	var orgID string
	if dbType == "postgres" {
		org := &drevtypes.Org{
			Name:             "Acme CI",
			Slug:             "acme-ci",
			Plan:             "team",
			WorkerLimit:      3,
			QueueLimit:       50,
			LogRetentionDays: 30,
		}
		err := s.CreateOrg(ctx, org)
		if err != nil {
			// If already exists, fetch it
			existingOrg, getErr := s.GetOrgBySlug(ctx, org.Slug)
			if getErr != nil {
				log.Fatalf("failed to create or get org: %v", err)
			}
			org = existingOrg
			fmt.Printf("Using existing Organization: %s (ID: %s)\n", org.Name, org.ID)
		} else {
			fmt.Printf("Created Organization: %s (ID: %s)\n", org.Name, org.ID)
		}
		orgID = org.ID

		// Insert API Token matching NEXT_PUBLIC_API_TOKEN
		rawToken := "e9cbb3489e1ad2db85aa14537a25b2be600d4e061bcc2f63ce4e85f463e1ee94"
		tokenHash := auth.HashToken(rawToken)

		token := &drevtypes.APIToken{
			OrgID:     orgID,
			Name:      "Dashboard Token",
			TokenHash: tokenHash,
			CreatedBy: "seeder",
		}
		err = s.CreateToken(ctx, token)
		if err != nil {
			fmt.Println("Dashboard API token already seeded.")
		} else {
			fmt.Println("Seeded Dashboard API Token successfully.")
		}
	}

	// 2. Create sample runs with various statuses
	runs := []struct {
		pipeline string
		status   drevtypes.RunStatus
		commit   string
		msg      string
		branch   string
		jobs     []string
	}{
		{
			pipeline: "drev-ci-pipeline",
			status:   drevtypes.StatusSuccess,
			commit:   "8f2c3a5e1d7b4c9a6f8e2d0c8b6a4f2e0d8c6b4a",
			msg:      "feat: support supabase token validation and monthly usage tracking",
			branch:   "main",
			jobs:     []string{"lint", "test", "build", "deploy"},
		},
		{
			pipeline: "frontend-dashboard",
			status:   drevtypes.StatusFailed,
			commit:   "3a9c6f2e8d0b4a6c8e0d2f4b6a8c0e2d4f6b8a0c",
			msg:      "fix: typescript compiling error in navbar.tsx",
			branch:   "patch-1",
			jobs:     []string{"install", "build"},
		},
		{
			pipeline: "drev-ci-pipeline",
			status:   drevtypes.StatusRunning,
			commit:   "4f2e0d8c6b4a8f2c3a5e1d7b4c9a6f8e2d0c8b6a",
			msg:      "chore: update start-drev.ps1 with Mode parameter support",
			branch:   "dev",
			jobs:     []string{"lint", "test", "build"},
		},
		{
			pipeline: "api-backend",
			status:   drevtypes.StatusPending,
			commit:   "7b4c9a6f8e2d0c8b6a4f2e0d8c6b4a8f2c3a5e1d",
			msg:      "docs: add setup instructions for local development mode",
			branch:   "main",
			jobs:     []string{"test", "build"},
		},
	}

	for i, r := range runs {
		runID := uuid.New().String()
		run := &drevtypes.Run{
			ID:           runID,
			OrgID:        orgID,
			PipelineID:   r.pipeline,
			PipelineName: r.pipeline,
			Status:       r.status,
			TriggeredBy:  "git-push",
			CommitSHA:    r.commit,
			CommitMsg:    r.msg,
			Branch:       r.branch,
			StartedAt:    time.Now().Add(-time.Duration(10-i) * time.Hour),
		}
		if r.status == drevtypes.StatusSuccess || r.status == drevtypes.StatusFailed {
			run.FinishedAt = run.StartedAt.Add(3 * time.Minute)
		}

		if err := s.CreateRun(ctx, run); err != nil {
			log.Fatalf("failed to create run: %v", err)
		}

		for _, jobName := range r.jobs {
			jobStatus := r.status
			if r.status == drevtypes.StatusRunning {
				if jobName == "lint" {
					jobStatus = drevtypes.StatusSuccess
				} else if jobName == "test" {
					jobStatus = drevtypes.StatusRunning
				} else {
					jobStatus = drevtypes.StatusPending
				}
			} else if r.status == drevtypes.StatusFailed {
				if jobName == "install" {
					jobStatus = drevtypes.StatusSuccess
				} else {
					jobStatus = drevtypes.StatusFailed
				}
			}

			job := &drevtypes.RunJob{
				ID:        uuid.New().String(),
				RunID:     runID,
				JobName:   jobName,
				Status:    jobStatus,
				StartedAt: run.StartedAt,
			}
			if jobStatus == drevtypes.StatusSuccess || jobStatus == drevtypes.StatusFailed {
				job.FinishedAt = job.StartedAt.Add(1 * time.Minute)
			}

			if err := s.CreateRunJob(ctx, job); err != nil {
				log.Fatalf("failed to create run job: %v", err)
			}
		}

		// Record usage events
		if dbType == "postgres" && orgID != "" {
			if err := s.RecordUsage(ctx, orgID, "pipeline_run", runID); err != nil {
				log.Printf("failed to record usage: %v", err)
			}
		}

		fmt.Printf("  > Seeded run %s (%s)\n", run.ID, r.pipeline)
	}

	fmt.Println("Database seeded successfully with premium mock CI data!")
}
