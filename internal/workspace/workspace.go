package workspace

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
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

	fmt.Fprintf(logWriter, "[drev] initializing workspace via Local Copy (DREV_LOCAL_REPO is set)\n")

	localPath := os.Getenv("DREV_LOCAL_REPO")
	if localPath == "" {
		return fmt.Errorf("DREV_LOCAL_REPO environment variable is not set")
	}

	// Use Windows 'robocopy' to efficiently copy the folder (bypassing node_modules and .git)
	// robocopy <source> <dest> /E /XF <files> /XD <dirs>
	robocopy := exec.CommandContext(cloneCtx, "robocopy", localPath, w.Dir, "/E", "/XD", ".git", "node_modules", "bin", "/NFL", "/NDL", "/NJH", "/NJS", "/nc", "/ns", "/np")
	
	// Robocopy exit codes 0-7 are success (it's weird)
	err := robocopy.Run()
	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() > 7 {
			return fmt.Errorf("robocopy failed with exit code %d", exitErr.ExitCode())
		}
	} else if err != nil {
		return fmt.Errorf("running robocopy: %w", err)
	}

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
