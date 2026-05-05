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

// Workspace manages the lifecycle of a build directory using turbo-optimized git.
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

	// 1. Create a sub-context with a timeout
	cloneCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	fmt.Fprintf(logWriter, "[drev] initializing workspace for %s @ %s\n", source.URL, ref)

	runGit := func(args ...string) error {
		cmd := exec.CommandContext(cloneCtx, "git", args...)
		cmd.Dir = w.Dir
		cmd.Stdout = logWriter
		cmd.Stderr = logWriter
		cmd.Env = append(os.Environ(), 
			"GIT_TERMINAL_PROMPT=0",
			"GIT_ASKPASS=echo",
			"GCM_INTERACTIVE=never",
			"GIT_TRACE=1",
			"GIT_CURL_VERBOSE=1",
			"GIT_HTTP_LOW_SPEED_LIMIT=1000",
			"GIT_HTTP_LOW_SPEED_TIME=30",
		)
		return cmd.Run()
	}

	// Manual sequence (Optimized for Windows performance)
	steps := [][]string{
		{"init"},
		{"config", "core.fscache", "true"},
		{"config", "core.preloadindex", "true"},
		{"-c", "credential.helper=", "remote", "add", "origin", source.URL},
		{
			"-c", "core.compression=0", 
			"-c", "pack.threads=1", 
			"-c", "credential.helper=", 
			"fetch", "--no-tags", "--no-recurse-submodules", "--filter=blob:none", "--depth", "1", "origin", ref,
		},
		{"checkout", "--progress", "FETCH_HEAD"},
	}

	for _, args := range steps {
		if err := runGit(args...); err != nil {
			return fmt.Errorf("git %s failed: %w", args[0], err)
		}
	}

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
