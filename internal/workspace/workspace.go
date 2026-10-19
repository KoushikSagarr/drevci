package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// Workspace manages the lifecycle of a build directory.
type Workspace struct {
	Dir string
}

// Create initializes a new pipeline-level root workspace directory.
func Create() (*Workspace, error) {
	dir, err := os.MkdirTemp("", "drev-workspace-*")
	if err != nil {
		return nil, fmt.Errorf("creating workspace dir: %w", err)
	}
	return &Workspace{Dir: dir}, nil
}

// SourceDir returns the path to the central immutable source checkout directory.
func (w *Workspace) SourceDir() string {
	return filepath.Join(w.Dir, "source")
}

// JobsDir returns the path to the directory hosting all individual job workspaces.
func (w *Workspace) JobsDir() string {
	return filepath.Join(w.Dir, "jobs")
}

// JobDir returns the path to the isolated workspace for the named job.
func (w *Workspace) JobDir(jobName string) string {
	return filepath.Join(w.JobsDir(), jobName)
}

// sanitizeJobName validates that jobName is a clean single directory name
// and prevents path traversal out of the jobs directory.
func sanitizeJobName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("job name cannot be empty")
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == "/" || strings.Contains(clean, "..") || strings.ContainsAny(clean, `/\:`) {
		return "", fmt.Errorf("invalid job name %q: contains unsafe path characters", name)
	}
	return clean, nil
}

// CreateJobWorkspace creates an isolated workspace for a job under w.JobsDir().
// If parentJob is non-empty, the workspace is populated as an independent snapshot
// of that parent job's completed workspace.
// Otherwise, it is populated as an independent copy of the central source directory.
func (w *Workspace) CreateJobWorkspace(jobName string, parentJob string) (*Workspace, error) {
	cleanJob, err := sanitizeJobName(jobName)
	if err != nil {
		return nil, err
	}

	targetDir := w.JobDir(cleanJob)
	if err := os.MkdirAll(filepath.Dir(targetDir), 0755); err != nil {
		return nil, fmt.Errorf("creating jobs directory: %w", err)
	}

	var srcDir string
	if parentJob != "" {
		cleanParent, err := sanitizeJobName(parentJob)
		if err != nil {
			return nil, fmt.Errorf("invalid parent job name: %w", err)
		}
		srcDir = w.JobDir(cleanParent)
		fi, err := os.Stat(srcDir)
		if err != nil || !fi.IsDir() {
			return nil, fmt.Errorf("parent workspace %q is not available: %w", parentJob, err)
		}
	} else {
		srcDir = w.SourceDir()
		fi, err := os.Stat(srcDir)
		if err != nil || !fi.IsDir() {
			if err := os.MkdirAll(srcDir, 0755); err != nil {
				return nil, fmt.Errorf("creating source dir: %w", err)
			}
		}
	}

	// Copy from srcDir to targetDir
	if err := CopyDir(srcDir, targetDir); err != nil {
		_ = os.RemoveAll(targetDir)
		return nil, fmt.Errorf("initializing job workspace %q: %w", jobName, err)
	}

	return &Workspace{Dir: targetDir}, nil
}

// Clone populates the central source directory w.SourceDir() from the repository source.
// After checkout, the central source directory is treated as immutable.
func (w *Workspace) Clone(ctx context.Context, source drevtypes.Source, logWriter io.Writer) error {
	sourceDir := w.SourceDir()
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		return fmt.Errorf("creating source dir: %w", err)
	}

	if source.URL == "" {
		return nil
	}

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
		if err := copyLocalFiltered(localPath, sourceDir); err != nil {
			return fmt.Errorf("local copy failed: %w", err)
		}
		return nil
	}

	// NORMAL LOGIC: Use standard Git clone for everything else
	fmt.Fprintf(logWriter, "[drev] cloning %s @ %s (Hybrid Mode: Standard Pipeline)\n", source.URL, ref)
	cmd := exec.CommandContext(cloneCtx, "git", "clone", "--depth", "1", "--branch", ref, source.URL, sourceDir)
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}
	return nil
}

// Cleanup removes the entire workspace tree including source and all job workspaces.
func (w *Workspace) Cleanup() error {
	return os.RemoveAll(w.Dir)
}

// CopyDir recursively copies a directory tree from src to dst.
// Symlinks, file permissions, and directory structures are preserved.
func CopyDir(src, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("reading source dir %q: %w", src, err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("source %q is not a directory", src)
	}

	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("creating destination dir %q: %w", dst, err)
	}

	err = filepath.WalkDir(src, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == src {
			return nil
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(dst, relPath)

		// Path traversal guard
		if !strings.HasPrefix(targetPath, dst+string(filepath.Separator)) && targetPath != dst {
			return fmt.Errorf("path %q escapes destination directory %q", targetPath, dst)
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("getting file info for %q: %w", path, err)
		}

		// Handle symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("reading symlink %q: %w", path, err)
			}
			_ = os.Remove(targetPath)
			if err := os.Symlink(linkTarget, targetPath); err != nil {
				return fmt.Errorf("creating symlink %q -> %q: %w", targetPath, linkTarget, err)
			}
			return nil
		}

		// Handle directories
		if d.IsDir() {
			perm := info.Mode().Perm() | 0700
			if err := os.MkdirAll(targetPath, perm); err != nil {
				return fmt.Errorf("creating directory %q: %w", targetPath, err)
			}
			return nil
		}

		// Handle regular files
		return copyFile(path, targetPath, info.Mode().Perm())
	})

	if err != nil {
		_ = os.RemoveAll(dst)
		return err
	}
	return nil
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening file %q: %w", src, err)
	}
	defer in.Close()

	// Ensure file is writable by owner so container jobs can write/overwrite
	outPerm := perm | 0600
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, outPerm)
	if err != nil {
		return fmt.Errorf("creating file %q: %w", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copying %q to %q: %w", src, dst, err)
	}

	return out.Sync()
}

// copyLocalFiltered copies files from src to dst excluding .git, node_modules, and bin.
func copyLocalFiltered(src, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == src {
			return nil
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		topPart := strings.Split(relPath, string(filepath.Separator))[0]
		if topPart == ".git" || topPart == "node_modules" || topPart == "bin" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		targetPath := filepath.Join(dst, relPath)
		info, err := d.Info()
		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			_ = os.Remove(targetPath)
			return os.Symlink(linkTarget, targetPath)
		}

		if d.IsDir() {
			return os.MkdirAll(targetPath, info.Mode().Perm()|0700)
		}

		return copyFile(path, targetPath, info.Mode().Perm())
	})
}
