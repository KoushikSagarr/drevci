package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// Workspace manages the lifecycle of a build directory with high resilience.
type Workspace struct {
	Dir string
}

func Create() (*Workspace, error) {
	dir, err := os.MkdirTemp("", "drev-workspace-*")
	if err != nil {
		return nil, fmt.Errorf("creating workspace dir: %w", err)
	}
	return &Workspace{Dir: dir}, nil
}

func (w *Workspace) Clone(ctx context.Context, source drevtypes.Source, logWriter io.Writer) error {
	if source.URL == "" {
		return nil
	}

	ref := source.Ref
	if ref == "" {
		ref = "main"
	}

	// 1. Create a sub-context with a timeout to prevent infinite hangs
	cloneCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	fmt.Fprintf(logWriter, "[drev] cloning %s @ %s\n", source.URL, ref)

	// Helper to run git commands with proper environment
	runGit := func(args ...string) error {
		cmd := exec.CommandContext(cloneCtx, "git", args...)
		cmd.Dir = w.Dir
		cmd.Stdout = logWriter
		cmd.Stderr = logWriter
		// Ensure Git never prompts for credentials or UI (which causes hangs)
		cmd.Env = append(os.Environ(), 
			"GIT_TERMINAL_PROMPT=0",
			"GIT_ASKPASS=echo",
			"GCM_INTERACTIVE=never",
		)
		return cmd.Run()
	}

	// Attempt 1: Fast shallow treeless clone of specific branch
	// Using -c to disable credential helpers and UI prompts to prevent hangs on Windows
	if err := runGit("-c", "core.compression=0", "-c", "pack.threads=1", "-c", "credential.helper=", "clone", "--filter=blob:none", "--depth", "1", "--branch", ref, source.URL, "."); err != nil {
		fmt.Fprintf(logWriter, "[drev] branch clone failed, cleaning up and retrying: %v\n", err)
		
		// 2. CRITICAL: Clean the directory before retrying
		entries, _ := os.ReadDir(w.Dir)
		for _, entry := range entries {
			os.RemoveAll(fmt.Sprintf("%s/%s", w.Dir, entry.Name()))
		}

		// Attempt 2: Full shallow treeless clone
		if err2 := runGit("-c", "core.compression=0", "-c", "pack.threads=1", "-c", "credential.helper=", "clone", "--filter=blob:none", "--depth", "1", source.URL, "."); err2 != nil {
			return fmt.Errorf("git clone failed: %w", err2)
		}

		// Attempt 3: Checkout specific ref
		if err3 := runGit("checkout", ref); err3 != nil {
			return fmt.Errorf("git checkout failed: %w", err3)
		}
	}

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
