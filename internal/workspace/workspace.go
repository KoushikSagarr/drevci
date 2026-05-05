package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/drevci/drev/pkg/drevtypes"
)

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

	fmt.Fprintf(logWriter, "[drev] cloning %s @ %s\n", source.URL, ref)

	cmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "--branch", ref, source.URL, ".")
	cmd.Dir = w.Dir
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter

	if err := cmd.Run(); err != nil {
		fmt.Fprintf(logWriter, "[drev] branch clone failed, trying default clone and checkout: %v\n", err)
		
		cmd2 := exec.CommandContext(ctx, "git", "clone", "--depth", "1", source.URL, ".")
		cmd2.Dir = w.Dir
		cmd2.Stdout = logWriter
		cmd2.Stderr = logWriter
		if err2 := cmd2.Run(); err2 != nil {
			return fmt.Errorf("git clone failed: %w", err2)
		}

		cmd3 := exec.CommandContext(ctx, "git", "checkout", ref)
		cmd3.Dir = w.Dir
		cmd3.Stdout = logWriter
		cmd3.Stderr = logWriter
		if err3 := cmd3.Run(); err3 != nil {
			return fmt.Errorf("git checkout failed: %w", err3)
		}
	}

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
