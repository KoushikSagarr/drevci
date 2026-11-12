package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

var credentialURLPattern = regexp.MustCompile(`([a-zA-Z0-9+.-]+://)([^/\s]+@)`)

// maskCredentials redacts userinfo/passwords from URL strings so they are never printed in logs or error messages.
func maskCredentials(s string) string {
	return credentialURLPattern.ReplaceAllString(s, "${1}***@")
}

type sanitizedWriter struct {
	w io.Writer
}

func (sw *sanitizedWriter) Write(p []byte) (n int, err error) {
	sanitized := maskCredentials(string(p))
	_, err = sw.w.Write([]byte(sanitized))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// isCommitSHA returns true if the string is a 40-character (SHA-1) or 64-character (SHA-256) hexadecimal hash.
func isCommitSHA(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

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
// It supports exact commit SHAs, PR refs (refs/pull/<id>/head), and branch/tag names.
// When an exact commit SHA is provided, it verifies that the checked-out HEAD matches
// the requested SHA and fails explicitly on mismatch.
// After checkout, the central source directory is treated as immutable.
func (w *Workspace) Clone(ctx context.Context, source drevtypes.Source, logWriter io.Writer) (cloneErr error) {
	if logWriter == nil {
		logWriter = io.Discard
	}
	sw := &sanitizedWriter{w: logWriter}

	sourceDir := w.SourceDir()

	if source.URL == "" {
		return nil
	}

	cloneCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// HYBRID LOGIC: If it's the Drev Self-Test (this repo), use Local Copy Bypass
	localPath := os.Getenv("DREV_LOCAL_REPO")
	if localPath != "" && strings.Contains(source.URL, "drevci") {
		fmt.Fprintf(sw, "[drev] initializing workspace via Local Copy (Hybrid Mode: Self-Test detected)\n")
		if err := os.MkdirAll(sourceDir, 0755); err != nil {
			return fmt.Errorf("creating source dir: %w", err)
		}
		if err := copyLocalFiltered(localPath, sourceDir); err != nil {
			return fmt.Errorf("local copy failed: %w", err)
		}
		return nil
	}

	// Clean any previous or partially initialized directory before checkout
	_ = os.RemoveAll(sourceDir)
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		return fmt.Errorf("creating source dir: %w", err)
	}

	// Always clean up sourceDir on failure
	defer func() {
		if cloneErr != nil {
			_ = os.RemoveAll(sourceDir)
			cloneErr = errors.New(maskCredentials(cloneErr.Error()))
		}
	}()

	rawRef := strings.TrimSpace(source.Ref)
	if strings.HasPrefix(rawRef, "-") {
		cloneErr = fmt.Errorf("git clone failed: invalid ref %q: ref name cannot start with '-'", rawRef)
		return cloneErr
	}

	var expectedSHA string
	var fetchTarget string

	if isCommitSHA(rawRef) {
		expectedSHA = strings.ToLower(rawRef)
		fetchTarget = rawRef
	} else if atIdx := strings.Index(rawRef, "@"); atIdx != -1 {
		prefix := strings.TrimSpace(rawRef[:atIdx])
		target := strings.TrimSpace(rawRef[atIdx+1:])
		if strings.HasPrefix(prefix, "-") || strings.HasPrefix(target, "-") {
			cloneErr = fmt.Errorf("git clone failed: invalid ref %q: ref name cannot start with '-'", rawRef)
			return cloneErr
		}
		if isCommitSHA(target) {
			expectedSHA = strings.ToLower(target)
			if prefix != "" {
				fetchTarget = prefix
			} else {
				fetchTarget = target
			}
		} else {
			fetchTarget = rawRef
		}
	} else if strings.HasPrefix(rawRef, "refs/") {
		fetchTarget = rawRef
	} else if rawRef != "" {
		fetchTarget = rawRef
	} else {
		fetchTarget = "main"
	}

	if strings.HasPrefix(fetchTarget, "-") {
		cloneErr = fmt.Errorf("git clone failed: invalid ref %q: ref name cannot start with '-'", fetchTarget)
		return cloneErr
	}

	// 1. Initialize git repository
	initCmd := exec.CommandContext(cloneCtx, "git", "init", "-q")
	initCmd.Dir = sourceDir
	var initErrBuf bytes.Buffer
	initCmd.Stdout = sw
	initCmd.Stderr = io.MultiWriter(sw, &initErrBuf)
	if err := initCmd.Run(); err != nil {
		cloneErr = fmt.Errorf("git clone failed: git init: %w (%s)", err, strings.TrimSpace(initErrBuf.String()))
		return cloneErr
	}

	// 2. Add remote origin
	remoteCmd := exec.CommandContext(cloneCtx, "git", "remote", "add", "origin", source.URL)
	remoteCmd.Dir = sourceDir
	var remoteErrBuf bytes.Buffer
	remoteCmd.Stdout = sw
	remoteCmd.Stderr = io.MultiWriter(sw, &remoteErrBuf)
	if err := remoteCmd.Run(); err != nil {
		cloneErr = fmt.Errorf("git clone failed: remote add: %w (%s)", err, strings.TrimSpace(remoteErrBuf.String()))
		return cloneErr
	}

	// 3. Fetch target revision
	if expectedSHA != "" {
		fmt.Fprintf(sw, "[drev] fetching revision %s from %s (target SHA: %s)\n", fetchTarget, maskCredentials(source.URL), expectedSHA)
	} else {
		fmt.Fprintf(sw, "[drev] fetching %s from %s\n", fetchTarget, maskCredentials(source.URL))
	}

	var fetchErrBuf bytes.Buffer
	fetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "--depth", "1", "origin", "--", fetchTarget)
	fetchCmd.Dir = sourceDir
	fetchCmd.Stdout = sw
	fetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
	if err := fetchCmd.Run(); err != nil {
		// Retry with full fetch if shallow fetch failed
		fullFetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "origin", "--", fetchTarget)
		fullFetchCmd.Dir = sourceDir
		fullFetchCmd.Stdout = sw
		fullFetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
		if err2 := fullFetchCmd.Run(); err2 != nil {
			cloneErr = fmt.Errorf("git clone failed: fetch %s from %s: %w (%s)", fetchTarget, source.URL, err2, strings.TrimSpace(fetchErrBuf.String()))
			return cloneErr
		}
	}

	// 4. Checkout fetched commit in detached HEAD mode
	checkoutCmd := exec.CommandContext(cloneCtx, "git", "checkout", "--detach", "-q", "FETCH_HEAD")
	checkoutCmd.Dir = sourceDir
	var checkoutErrBuf bytes.Buffer
	checkoutCmd.Stdout = sw
	checkoutCmd.Stderr = io.MultiWriter(sw, &checkoutErrBuf)
	if err := checkoutCmd.Run(); err != nil {
		cloneErr = fmt.Errorf("git clone failed: checkout: %w (%s)", err, strings.TrimSpace(checkoutErrBuf.String()))
		return cloneErr
	}

	// 5. Verify resulting HEAD
	revParseCmd := exec.CommandContext(cloneCtx, "git", "rev-parse", "HEAD")
	revParseCmd.Dir = sourceDir
	var revParseOut bytes.Buffer
	var revParseErr bytes.Buffer
	revParseCmd.Stdout = &revParseOut
	revParseCmd.Stderr = &revParseErr
	if err := revParseCmd.Run(); err != nil {
		cloneErr = fmt.Errorf("git clone failed: rev-parse HEAD: %w (%s)", err, strings.TrimSpace(revParseErr.String()))
		return cloneErr
	}

	actualSHA := strings.TrimSpace(revParseOut.String())
	if expectedSHA != "" {
		if !strings.EqualFold(actualSHA, expectedSHA) {
			cloneErr = fmt.Errorf("git clone failed: commit mismatch: expected %s, got %s", expectedSHA, actualSHA)
			return cloneErr
		}
		fmt.Fprintf(sw, "[drev] successfully checked out commit %s\n", actualSHA)
	} else {
		if !isCommitSHA(actualSHA) {
			cloneErr = fmt.Errorf("git clone failed: invalid HEAD revision: %q", actualSHA)
			return cloneErr
		}
		fmt.Fprintf(sw, "[drev] successfully checked out %s @ %s\n", fetchTarget, actualSHA)
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
