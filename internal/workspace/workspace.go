package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// Workspace manages the lifecycle of a build directory.
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
	cloneCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	ref := source.Ref
	if ref == "" {
		ref = "main"
	}

	// HYBRID LOGIC: If it's the Drev Self-Test (this repo), use Local Copy Bypass
	localPath := os.Getenv("DREV_LOCAL_REPO")
	if localPath != "" && strings.Contains(source.URL, "drevci") {
		fmt.Fprintf(logWriter, "[drev] initializing workspace via Local Copy (Hybrid Mode: Self-Test detected)\n")
		// robocopy <source> <dest> /E /XD .git node_modules bin
		robocopy := exec.CommandContext(cloneCtx, "robocopy", localPath, w.Dir, "/E", "/XD", ".git", "node_modules", "bin", "/NFL", "/NDL", "/NJH", "/NJS", "/nc", "/ns", "/np")
		err := robocopy.Run()
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() <= 7 {
			return nil
		} else if err == nil {
			return nil
		}
		return fmt.Errorf("local copy failed: %w", err)
	}

	// NORMAL LOGIC: Use standard Git clone for everything else
	fmt.Fprintf(logWriter, "[drev] cloning %s @ %s (Hybrid Mode: Standard Pipeline)\n", source.URL, ref)
	cmd := exec.CommandContext(cloneCtx, "git", "clone", "--depth", "1", "--branch", ref, source.URL, w.Dir)
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	return cmd.Run()
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
