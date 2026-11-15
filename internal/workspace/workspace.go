package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// DefaultWorkspaceDir is the canonical fallback directory for pipeline checkouts.
const DefaultWorkspaceDir = "/tmp/drev-workspaces"

// RunStore is the subset of store.Store required by workspace cleanup to confirm run state.
type RunStore interface {
	GetRun(ctx context.Context, id string) (*drevtypes.Run, error)
}

func isTerminalRunStatus(status drevtypes.RunStatus) bool {
	return status == drevtypes.StatusSuccess || status == drevtypes.StatusFailed || status == drevtypes.StatusCancelled
}

// ResolveWorkspaceRoot returns the canonical, validated workspace root directory.
func ResolveWorkspaceRoot() (string, error) {
	raw := os.Getenv("DREV_WORKSPACE_DIR")
	if strings.TrimSpace(raw) == "" {
		raw = DefaultWorkspaceDir
	}
	clean := filepath.Clean(raw)
	if !filepath.IsAbs(clean) {
		return "", fmt.Errorf("workspace root must be an absolute path: %q", raw)
	}
	if clean == "/" || clean == "." {
		return "", fmt.Errorf("refusing to use %q as workspace root", clean)
	}

	if err := os.MkdirAll(clean, 0700); err != nil {
		return "", fmt.Errorf("creating workspace root: %w", err)
	}

	canonical, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("evaluating symlinks for workspace root: %w", err)
	}

	fi, err := os.Lstat(canonical)
	if err != nil {
		return "", fmt.Errorf("inspecting workspace root: %w", err)
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("workspace root %q must be a real directory, not a symlink", canonical)
	}

	return canonical, nil
}

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
	return drevtypes.IsCommitSHA(s)
}

// Workspace manages the lifecycle of a build directory.
type Workspace struct {
	Dir      string
	RunID    string
	lockFile *os.File
	store    RunStore
	wsRoot   string
}

// CreateForRun initializes an isolated pipeline workspace directory with active kernel locking and metadata.
func CreateForRun(rootDir, runID string, s RunStore) (*Workspace, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, errors.New("runID cannot be empty")
	}
	cleanRunID := filepath.Clean(runID)
	if strings.ContainsAny(cleanRunID, "/\\:\x00") || cleanRunID == "." || cleanRunID == ".." {
		return nil, fmt.Errorf("invalid run ID %q: contains unsafe path characters", runID)
	}

	canonicalRoot := rootDir
	if canonicalRoot == "" {
		resolved, err := ResolveWorkspaceRoot()
		if err != nil {
			return nil, err
		}
		canonicalRoot = resolved
	} else {
		clean := filepath.Clean(canonicalRoot)
		if !filepath.IsAbs(clean) {
			return nil, fmt.Errorf("workspace root must be an absolute path: %q", canonicalRoot)
		}
		if err := os.MkdirAll(clean, 0700); err != nil {
			return nil, fmt.Errorf("creating workspace root: %w", err)
		}
		eval, err := filepath.EvalSymlinks(clean)
		if err != nil {
			return nil, fmt.Errorf("evaluating symlinks for workspace root: %w", err)
		}
		canonicalRoot = eval
	}

	nonce := uuid.New().String()
	nonceShort := nonce[:8]
	dirName := fmt.Sprintf("drev-workspace-%s-%s", cleanRunID, nonceShort)
	dirPath := filepath.Join(canonicalRoot, dirName)

	if err := os.Mkdir(dirPath, 0700); err != nil {
		return nil, fmt.Errorf("creating workspace directory: %w", err)
	}

	lockPath := filepath.Join(dirPath, ".drev_lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		_ = os.RemoveAll(dirPath)
		return nil, fmt.Errorf("creating lock file: %w", err)
	}

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		_ = os.RemoveAll(dirPath)
		return nil, fmt.Errorf("acquiring workspace lock: %w", err)
	}

	meta := WorkspaceMetadata{
		Magic:         WorkspaceMetaMagic,
		SchemaVersion: WorkspaceMetaVersion,
		RunID:         cleanRunID,
		OriginalDir:   dirName,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		CreatorPID:    os.Getpid(),
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		_ = os.RemoveAll(dirPath)
		return nil, fmt.Errorf("marshaling workspace metadata: %w", err)
	}

	metaPath := filepath.Join(dirPath, ".drev_meta.json")
	if err := os.WriteFile(metaPath, metaBytes, 0600); err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		_ = os.RemoveAll(dirPath)
		return nil, fmt.Errorf("writing workspace metadata: %w", err)
	}

	return &Workspace{
		Dir:      dirPath,
		RunID:    cleanRunID,
		lockFile: lockFile,
		store:    s,
		wsRoot:   canonicalRoot,
	}, nil
}

// Create initializes a new pipeline-level root workspace directory.
// Provided for backward compatibility in standalone test environments.
func Create() (*Workspace, error) {
	tempRunID := "test-" + uuid.New().String()
	return CreateForRun("", tempRunID, nil)
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

	rawCommitSHA := strings.TrimSpace(source.CommitSHA)
	rawRef := strings.TrimSpace(source.Ref)

	if strings.HasPrefix(rawCommitSHA, "-") {
		cloneErr = fmt.Errorf("git clone failed: invalid commit SHA %q: cannot start with '-'", rawCommitSHA)
		return cloneErr
	}
	if strings.HasPrefix(rawRef, "-") {
		cloneErr = fmt.Errorf("git clone failed: invalid ref %q: ref name cannot start with '-'", rawRef)
		return cloneErr
	}

	var expectedSHA string
	var fetchTarget string
	var isDirectSHA bool

	if rawCommitSHA != "" {
		if !drevtypes.IsCommitSHA(rawCommitSHA) {
			cloneErr = fmt.Errorf("git clone failed: invalid commit SHA %q", rawCommitSHA)
			return cloneErr
		}
		expectedSHA = strings.ToLower(rawCommitSHA)
		fetchTarget = expectedSHA
		isDirectSHA = true
	} else if isCommitSHA(rawRef) {
		expectedSHA = strings.ToLower(rawRef)
		fetchTarget = rawRef
		isDirectSHA = true
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
				isDirectSHA = true
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

	var fetchSuccess bool
	var fetchErrBuf bytes.Buffer

	// If a direct commit SHA was requested (via CommitSHA or Ref), attempt direct SHA fetch
	if isDirectSHA {
		fetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "--depth", "1", "origin", "--", expectedSHA)
		fetchCmd.Dir = sourceDir
		fetchCmd.Stdout = sw
		fetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
		if err := fetchCmd.Run(); err == nil {
			fetchSuccess = true
		} else {
			fullFetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "origin", "--", expectedSHA)
			fullFetchCmd.Dir = sourceDir
			fullFetchCmd.Stdout = sw
			fullFetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
			if err2 := fullFetchCmd.Run(); err2 == nil {
				fetchSuccess = true
			}
		}

		// Fallback: if direct SHA fetch failed on a server that disallows fetching raw SHAs,
		// and a named ref was provided, try fetching that ref
		if !fetchSuccess && rawRef != "" && rawRef != expectedSHA {
			refToFetch := rawRef
			if atIdx := strings.Index(rawRef, "@"); atIdx != -1 {
				refToFetch = strings.TrimSpace(rawRef[:atIdx])
			}
			if refToFetch != "" && !strings.HasPrefix(refToFetch, "-") {
				refFetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "--depth", "1", "origin", "--", refToFetch)
				refFetchCmd.Dir = sourceDir
				refFetchCmd.Stdout = sw
				refFetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
				if err := refFetchCmd.Run(); err == nil {
					fetchSuccess = true
				} else {
					fullRefFetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "origin", "--", refToFetch)
					fullRefFetchCmd.Dir = sourceDir
					fullRefFetchCmd.Stdout = sw
					fullRefFetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
					if err2 := fullRefFetchCmd.Run(); err2 == nil {
						fetchSuccess = true
					}
				}
			}
		}

		if !fetchSuccess {
			cloneErr = fmt.Errorf("git clone failed: fetch %s from %s: %s", expectedSHA, source.URL, strings.TrimSpace(fetchErrBuf.String()))
			return cloneErr
		}
	} else {
		// Non-direct-SHA fetch (e.g. named branch, PR ref, or <ref>@<sha>): fetch fetchTarget
		fetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "--depth", "1", "origin", "--", fetchTarget)
		fetchCmd.Dir = sourceDir
		fetchCmd.Stdout = sw
		fetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
		if err := fetchCmd.Run(); err == nil {
			fetchSuccess = true
		} else {
			fullFetchCmd := exec.CommandContext(cloneCtx, "git", "fetch", "origin", "--", fetchTarget)
			fullFetchCmd.Dir = sourceDir
			fullFetchCmd.Stdout = sw
			fullFetchCmd.Stderr = io.MultiWriter(sw, &fetchErrBuf)
			if err2 := fullFetchCmd.Run(); err2 == nil {
				fetchSuccess = true
			} else {
				cloneErr = fmt.Errorf("git clone failed: fetch %s from %s: %w (%s)", fetchTarget, source.URL, err2, strings.TrimSpace(fetchErrBuf.String()))
				return cloneErr
			}
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
// Workspace cleanup is strictly conditional on confirmed database terminal status.
// If the run is not confirmed terminal (e.g. database unavailable or terminal-state update failed),
// the workspace directory is preserved.
func (w *Workspace) Cleanup() error {
	if w.Dir == "" {
		return nil
	}

	// Rule 2: Workspace cleanup is conditional on confirmed database authorization
	if w.store != nil && w.RunID != "" {
		run, err := w.store.GetRun(context.Background(), w.RunID)
		if err != nil || run == nil || !isTerminalRunStatus(run.Status) {
			// Database unavailable, persistence failed, or run is still pending/running:
			// PRESERVE WORKSPACE. Do not delete directory. Do not publish tombstone.
			// Release flock so process can exit cleanly and on reboot ResetGhostRuns
			// can mark it failed and startup reconciliation can safely reclaim it.
			if w.lockFile != nil {
				_ = syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
				_ = w.lockFile.Close()
				w.lockFile = nil
			}
			if err != nil {
				return fmt.Errorf("cleanup aborted: database unavailable or terminal status not confirmed: %w", err)
			}
			if run != nil {
				return fmt.Errorf("cleanup aborted: run %s is in non-terminal status %s", w.RunID, run.Status)
			}
			return fmt.Errorf("cleanup aborted: run %s not found in store", w.RunID)
		}
	}

	// If no lockFile or wsRoot exists (e.g. legacy fallback or mock), perform basic RemoveAll
	if w.lockFile == nil || w.wsRoot == "" {
		return os.RemoveAll(w.Dir)
	}

	origName := filepath.Base(w.Dir)
	if !strings.HasPrefix(origName, "drev-workspace-") {
		return os.RemoveAll(w.Dir)
	}
	targetName := strings.Replace(origName, "drev-workspace-", "drev-deleting-", 1)

	// Open workspace root directory descriptor
	rootFd, err := unix.Open(w.wsRoot, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("opening workspace root descriptor: %w", err)
	}
	defer unix.Close(rootFd)

	// Step 1: Publish tombstone BEFORE renaming directory
	tombstoneID := uuid.New().String()
	ts := Tombstone{
		Magic:          TombstoneMagic,
		SchemaVersion:  TombstoneVersion,
		TombstoneID:    tombstoneID,
		RunID:          w.RunID,
		OriginalDir:    origName,
		TargetDir:      targetName,
		DeletionIntent: DeletionIntentCleanup,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		CreatorPID:     os.Getpid(),
	}

	tombF, err := PublishTombstone(w.wsRoot, ts)
	if err != nil {
		return fmt.Errorf("publishing deletion tombstone: %w", err)
	}
	defer func() {
		if tombF != nil {
			_ = syscall.Flock(int(tombF.Fd()), syscall.LOCK_UN)
			_ = tombF.Close()
		}
	}()

	// Step 2: Atomic no-replace transition: drev-workspace-* -> drev-deleting-*
	if err := renameNoReplaceAt(rootFd, origName, rootFd, targetName); err != nil {
		// Failed to rename (e.g. target already existed); preserve tombstone and original directory
		return fmt.Errorf("atomic rename to deleting namespace failed: %w", err)
	}
	_ = unix.Fsync(rootFd)

	// Update w.Dir to target path
	w.Dir = filepath.Join(w.wsRoot, targetName)

	// Close workspace lock descriptor now that directory is renamed and tombstone lock is held
	_ = syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
	_ = w.lockFile.Close()
	w.lockFile = nil

	// Step 3: Descriptor-relative recursive purge anchored to rootFd
	if err := removeDirectoryUnderRoot(rootFd, targetName); err != nil {
		// Purge failed partially: tombstone remains intact for startup recovery!
		return fmt.Errorf("descriptor-relative purge of %s: %w", targetName, err)
	}
	_ = unix.Fsync(rootFd)

	// Step 4: Unlink tombstone file upon confirmed directory removal
	tombstoneDir := filepath.Join(w.wsRoot, ".tombstones")
	tombDirFd, err := unix.Open(tombstoneDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err == nil {
		_ = unix.Unlinkat(tombDirFd, tombstoneID+".tombstone", 0)
		_ = unix.Fsync(tombDirFd)
		_ = unix.Close(tombDirFd)
	}

	w.Dir = ""
	return nil
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
