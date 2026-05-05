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

	//func (w *Workspace) Clone(ctx context.Context, source drevtypes.Repository, ref string, logWriter io.Writer) error {
	cloneCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	fmt.Fprintf(logWriter, "[drev] cloning %s @ %s\n", source.URL, ref)
	cmd := exec.CommandContext(cloneCtx, "git", "clone", "--depth", "1", "--branch", ref, source.URL, w.Dir)
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	return cmd.Run()
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
