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

	fmt.Fprintf(logWriter, "[drev] initializing workspace via ZIP download (bypassing Git binary)\n")

	// Convert git URL to ZIP URL: https://github.com/user/repo/archive/refs/heads/main.zip
	zipURL := strings.TrimSuffix(source.URL, ".git") + "/archive/refs/heads/" + ref + ".zip"
	
	resp, err := http.Get(zipURL)
	if err != nil {
		return fmt.Errorf("downloading repo zip: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download zip: status %d", resp.StatusCode)
	}

	zipFile := fmt.Sprintf("%s/repo.zip", w.Dir)
	out, err := os.Create(zipFile)
	if err != nil {
		return fmt.Errorf("creating zip file: %w", err)
	}
	
	_, err = io.Copy(out, resp.Body)
	out.Close()
	if err != nil {
		return fmt.Errorf("saving zip body: %w", err)
	}

	// Extract using Windows built-in tar (which supports zip)
	tarCmd := exec.CommandContext(cloneCtx, "tar", "-xf", "repo.zip", "--strip-components=1")
	tarCmd.Dir = w.Dir
	if err := tarCmd.Run(); err != nil {
		return fmt.Errorf("extraction failed: %w", err)
	}
	
	os.Remove(zipFile)
	return nil
}

func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}
