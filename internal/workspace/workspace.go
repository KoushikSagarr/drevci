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

// Workspace manages the lifecycle of a build directory in a stable, optimized location.
type Workspace struct {
	Dir string
}

func Create() (*Workspace, error) {
	// Use a dedicated root directory to avoid OneDrive/Sync issues in User folders
	baseDir := `C:\drev-workspaces`
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		// Fallback to system temp if C:\ is not writable
		baseDir = "" 
	}
	
	dir, err := os.MkdirTemp(baseDir, "drev-workspace-*")
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

	// 1. Create a sub-context with a timeout
	cloneCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	fmt.Fprintf(logWriter, "[drev] initializing workspace for %s @ %s\n", source.URL, ref)

	// Use the hardened clone command in the safe zone
	args := []string{
		"-c", "core.autocrlf=false",
		"-c", "core.fscache=true",
		"-c", "gc.auto=0",
		"-c", "core.fsmonitor=false",
		"clone",
		"--depth", "1",
		"--no-tags",
		"--single-branch",
		"--branch", ref,
		source.URL,
		w.Dir,
	}

	cmd := exec.CommandContext(cloneCtx, "git", args...)
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	
	cmd.Env = append(os.Environ(), 
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=echo",
		"GCM_INTERACTIVE=never",
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
