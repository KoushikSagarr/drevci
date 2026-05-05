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

	runGit := func(args ...string) error {
		cmd := exec.CommandContext(cloneCtx, "git", args...)
		cmd.Dir = w.Dir
		
		// Use a simple pipe to read logs and write them to the logWriter in the background
		// This prevents the logWriter (streamer) from blocking the Git process
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		
		if err := cmd.Start(); err != nil {
			return err
		}
		
		go io.Copy(logWriter, stdout)
		go io.Copy(logWriter, stderr)
		
		cmd.Env = append(os.Environ(), 
			"GIT_TERMINAL_PROMPT=0",
			"GIT_ASKPASS=echo",
			"GCM_INTERACTIVE=never",
		)
		return cmd.Wait()
	}

	// Manual sequence (Optimized for Windows performance)
	steps := [][]string{
		{"init"},
		{"config", "core.fscache", "true"},
		{"config", "core.preloadindex", "true"},
		{"config", "core.longpaths", "true"},
		{"-c", "credential.helper=", "remote", "add", "origin", source.URL},
		{
			"-c", "core.compression=0", 
			"-c", "pack.threads=1", 
			"-c", "credential.helper=", 
			"fetch", "--no-tags", "--no-recurse-submodules", "--filter=blob:none", "--depth", "1", "origin", ref,
		},
		// Stream files directly to disk (Bypasses Windows file-locking hangs)
		{"archive", "--format=tar", "FETCH_HEAD", "-o", "repo.tar"},
	}

	for _, args := range steps {
		if err := runGit(args...); err != nil {
			return fmt.Errorf("git %s failed: %w", args[0], err)
		}
	}

	// Extract the archive manually using Windows built-in tar
	tarCmd := exec.CommandContext(cloneCtx, "tar", "-xf", "repo.tar")
	tarCmd.Dir = w.Dir
	if err := tarCmd.Run(); err != nil {
		return fmt.Errorf("tar extraction failed: %w", err)
	}
	os.Remove(fmt.Sprintf("%s/repo.tar", w.Dir))

	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
