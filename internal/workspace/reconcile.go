package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// ReconciliationSummary details the outcome of an orphaned workspace reconciliation pass.
type ReconciliationSummary struct {
	TotalChecked int      // Total candidate directories and remnants inspected
	Removed      int      // Total orphaned workspace directories safely removed
	Skipped      int      // Workspaces skipped (active, unverified, or preserved)
	Errors       []string // Non-fatal errors encountered during reconciliation
}

// LogReconciliationSummary writes structured reconciliation messages to logFn.
func LogReconciliationSummary(logFn func(format string, v ...any), summary *ReconciliationSummary, topLevelErr error) {
	if logFn == nil {
		return
	}
	if topLevelErr != nil {
		logFn("Warning: orphaned workspace reconciliation failed: %v", topLevelErr)
	}
	if summary == nil {
		return
	}
	if len(summary.Errors) > 0 {
		for _, errStr := range summary.Errors {
			logFn("Warning: workspace reconciliation issue: %s", errStr)
		}
		logFn("Warning: orphaned workspace reconciliation completed with %d issue(s) (%d inspected, %d removed, %d skipped)",
			len(summary.Errors), summary.TotalChecked, summary.Removed, summary.Skipped)
	} else if summary.Removed > 0 {
		logFn("Reconciled %d orphaned Drev CI workspace(s) (%d inspected)", summary.Removed, summary.TotalChecked)
	}
}

// ReconcileOrphans executes a two-pass reconciliation over tombstone remnants and candidate workspaces.
func ReconcileOrphans(ctx context.Context, wsRoot string, s RunStore, interruptedRunIDs []string) (*ReconciliationSummary, error) {
	summary := &ReconciliationSummary{}

	canonicalRoot, err := filepath.EvalSymlinks(filepath.Clean(wsRoot))
	if err != nil {
		return summary, fmt.Errorf("canonicalizing workspace root: %w", err)
	}

	rootFd, err := unix.Open(canonicalRoot, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return summary, fmt.Errorf("opening workspace root descriptor: %w", err)
	}
	defer unix.Close(rootFd)

	tombstoneDir := filepath.Join(canonicalRoot, ".tombstones")
	_ = os.MkdirAll(tombstoneDir, 0700)

	tombDirFd, err := unix.Open(tombstoneDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return summary, fmt.Errorf("opening tombstone directory descriptor: %w", err)
	}
	defer unix.Close(tombDirFd)

	interruptedMap := make(map[string]bool, len(interruptedRunIDs))
	for _, id := range interruptedRunIDs {
		interruptedMap[id] = true
	}

	// ── PASS 1: Recover In-Flight Deletion Remnants (.tombstones/) ──────────────
	tombstoneEntries, err := os.ReadDir(tombstoneDir)
	if err == nil {
		for _, entry := range tombstoneEntries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tombstone") || strings.HasPrefix(entry.Name(), ".tmp-") {
				continue
			}

			summary.TotalChecked++
			tombName := entry.Name()

			tombFd, err := unix.Openat(tombDirFd, tombName, os.O_RDWR|unix.O_NOFOLLOW, 0600)
			if err != nil {
				summary.Skipped++
				summary.Errors = append(summary.Errors, fmt.Sprintf("cannot open tombstone %s (symlink or permission): %v", tombName, err))
				continue
			}

			// Lock tombstone descriptor
			if err := syscall.Flock(tombFd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				// Held by an active worker or deleter
				unix.Close(tombFd)
				summary.Skipped++
				continue
			}

			tombF := os.NewFile(uintptr(tombFd), tombName)
			data, err := os.ReadFile(filepath.Join(tombstoneDir, tombName))
			if err != nil {
				summary.Skipped++
				summary.Errors = append(summary.Errors, fmt.Sprintf("reading tombstone %s: %v", tombName, err))
				_ = syscall.Flock(tombFd, syscall.LOCK_UN)
				tombF.Close()
				continue
			}

			var ts Tombstone
			if err := json.Unmarshal(data, &ts); err != nil {
				summary.Skipped++
				summary.Errors = append(summary.Errors, fmt.Sprintf("unmarshaling tombstone %s: %v", tombName, err))
				_ = syscall.Flock(tombFd, syscall.LOCK_UN)
				tombF.Close()
				continue
			}

			if err := ValidateTombstone(&ts, tombName); err != nil {
				summary.Skipped++
				summary.Errors = append(summary.Errors, fmt.Sprintf("validating tombstone %s: %v", tombName, err))
				_ = syscall.Flock(tombFd, syscall.LOCK_UN)
				tombF.Close()
				continue
			}

			// Verify database authorization
			authorized := false
			if interruptedMap[ts.RunID] {
				authorized = true
			} else if s != nil {
				run, err := s.GetRun(ctx, ts.RunID)
				if err == nil && run != nil && isTerminalRunStatus(run.Status) {
					authorized = true
				} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
					summary.Errors = append(summary.Errors, fmt.Sprintf("store error checking run %s for tombstone %s: %v", ts.RunID, tombName, err))
				}
			}

			if !authorized {
				summary.Skipped++
				_ = syscall.Flock(tombFd, syscall.LOCK_UN)
				tombF.Close()
				continue
			}

			// Inspect physical original and target directories
			var origStat, targetStat unix.Stat_t
			origErr := unix.Fstatat(rootFd, ts.OriginalDir, &origStat, unix.AT_SYMLINK_NOFOLLOW)
			targetErr := unix.Fstatat(rootFd, ts.TargetDir, &targetStat, unix.AT_SYMLINK_NOFOLLOW)

			origExists := origErr == nil && (origStat.Mode&unix.S_IFMT == unix.S_IFDIR)
			targetExists := targetErr == nil && (targetStat.Mode&unix.S_IFMT == unix.S_IFDIR)

			if origExists && targetExists {
				summary.Skipped++
				summary.Errors = append(summary.Errors, fmt.Sprintf("conflict: both original %s and target %s exist for tombstone %s; preserving for manual review", ts.OriginalDir, ts.TargetDir, tombName))
				_ = syscall.Flock(tombFd, syscall.LOCK_UN)
				tombF.Close()
				continue
			}

			if origExists {
				// Crash occurred before directory rename: execute no-replace transition now
				if err := renameNoReplaceAt(rootFd, ts.OriginalDir, rootFd, ts.TargetDir); err != nil {
					summary.Errors = append(summary.Errors, fmt.Sprintf("renaming %s to %s for tombstone %s: %v", ts.OriginalDir, ts.TargetDir, tombName, err))
					_ = syscall.Flock(tombFd, syscall.LOCK_UN)
					tombF.Close()
					continue
				}
				_ = unix.Fsync(rootFd)
				targetExists = true
			}

			if targetExists {
				if err := removeDirectoryUnderRoot(rootFd, ts.TargetDir); err != nil {
					summary.Errors = append(summary.Errors, fmt.Sprintf("purging remnant directory %s: %v", ts.TargetDir, err))
					_ = syscall.Flock(tombFd, syscall.LOCK_UN)
					tombF.Close()
					continue
				}
				_ = unix.Fsync(rootFd)
			}

			// Unlink tombstone after confirmed directory removal
			if err := unix.Unlinkat(tombDirFd, tombName, 0); err != nil {
				summary.Errors = append(summary.Errors, fmt.Sprintf("unlinking tombstone %s: %v", tombName, err))
			} else {
				_ = unix.Fsync(tombDirFd)
				summary.Removed++
			}

			_ = syscall.Flock(tombFd, syscall.LOCK_UN)
			tombF.Close()
		}
	}

	// ── PASS 2: Reconcile Candidate Active Workspaces ──────────────────────────
	entries, err := os.ReadDir(canonicalRoot)
	if err != nil {
		return summary, fmt.Errorf("reading workspace root entries: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() {
			continue
		}

		if strings.HasPrefix(name, "drev-deleting-") {
			// Untracked deletion remnant without tombstone
			summary.Skipped++
			summary.Errors = append(summary.Errors, fmt.Sprintf("untracked deletion remnant %s (no tombstone); skipping", name))
			continue
		}

		if !strings.HasPrefix(name, "drev-workspace-") {
			continue
		}

		summary.TotalChecked++

		// Inspect candidate non-following
		var candStat unix.Stat_t
		if err := unix.Fstatat(rootFd, name, &candStat, unix.AT_SYMLINK_NOFOLLOW); err != nil || candStat.Mode&unix.S_IFMT != unix.S_IFDIR {
			summary.Skipped++
			continue
		}

		candFd, err := unix.Openat(rootFd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		if err != nil {
			summary.Skipped++
			continue
		}

		// Inspect lock file non-following
		lockFd, err := unix.Openat(candFd, ".drev_lock", os.O_RDWR|unix.O_NOFOLLOW, 0600)
		if err != nil {
			unix.Close(candFd)
			summary.Skipped++
			continue
		}

		// Try kernel advisory lock on candidate workspace
		if err := syscall.Flock(lockFd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			// Workspace is held by active worker!
			unix.Close(lockFd)
			unix.Close(candFd)
			summary.Skipped++
			continue
		}

		// Read and validate .drev_meta.json non-following
		metaFd, err := unix.Openat(candFd, ".drev_meta.json", os.O_RDONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			_ = syscall.Flock(lockFd, syscall.LOCK_UN)
			unix.Close(lockFd)
			unix.Close(candFd)
			summary.Skipped++
			summary.Errors = append(summary.Errors, fmt.Sprintf("workspace %s missing .drev_meta.json; skipping", name))
			continue
		}

		metaF := os.NewFile(uintptr(metaFd), ".drev_meta.json")
		metaData, err := os.ReadFile(filepath.Join(canonicalRoot, name, ".drev_meta.json"))
		metaF.Close()

		var meta WorkspaceMetadata
		if err != nil || json.Unmarshal(metaData, &meta) != nil || meta.Magic != WorkspaceMetaMagic || meta.RunID == "" || meta.OriginalDir != name {
			_ = syscall.Flock(lockFd, syscall.LOCK_UN)
			unix.Close(lockFd)
			unix.Close(candFd)
			summary.Skipped++
			summary.Errors = append(summary.Errors, fmt.Sprintf("workspace %s has corrupt metadata; skipping", name))
			continue
		}

		// Verify database authorization
		authorized := false
		if interruptedMap[meta.RunID] {
			authorized = true
		} else if s != nil {
			run, err := s.GetRun(ctx, meta.RunID)
			if err == nil && run != nil && isTerminalRunStatus(run.Status) {
				authorized = true
			} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
				summary.Errors = append(summary.Errors, fmt.Sprintf("store error checking run %s for workspace %s: %v", meta.RunID, name, err))
			}
		}

		if !authorized {
			_ = syscall.Flock(lockFd, syscall.LOCK_UN)
			unix.Close(lockFd)
			unix.Close(candFd)
			summary.Skipped++
			continue
		}

		// Authorized for deletion: Execute durable tombstone protocol
		tombstoneID := uuid.New().String()
		targetName := strings.Replace(name, "drev-workspace-", "drev-deleting-", 1)
		ts := Tombstone{
			Magic:          TombstoneMagic,
			SchemaVersion:  TombstoneVersion,
			TombstoneID:    tombstoneID,
			RunID:          meta.RunID,
			OriginalDir:    name,
			TargetDir:      targetName,
			DeletionIntent: DeletionIntentCleanup,
			CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
			CreatorPID:     os.Getpid(),
		}

		tombF, err := PublishTombstone(canonicalRoot, ts)
		if err != nil {
			summary.Errors = append(summary.Errors, fmt.Sprintf("publishing tombstone for %s: %v", name, err))
			_ = syscall.Flock(lockFd, syscall.LOCK_UN)
			unix.Close(lockFd)
			unix.Close(candFd)
			summary.Skipped++
			continue
		}

		// Atomic no-replace transition
		if err := renameNoReplaceAt(rootFd, name, rootFd, targetName); err != nil {
			summary.Errors = append(summary.Errors, fmt.Sprintf("renaming %s to %s: %v", name, targetName, err))
			_ = syscall.Flock(lockFd, syscall.LOCK_UN)
			unix.Close(lockFd)
			unix.Close(candFd)
			_ = syscall.Flock(int(tombF.Fd()), syscall.LOCK_UN)
			tombF.Close()
			summary.Skipped++
			continue
		}
		_ = unix.Fsync(rootFd)

		// Release old workspace lock and close candidate directory descriptor
		_ = syscall.Flock(lockFd, syscall.LOCK_UN)
		unix.Close(lockFd)
		unix.Close(candFd)

		// Purge target directory descriptor-relatively
		if err := removeDirectoryUnderRoot(rootFd, targetName); err != nil {
			summary.Errors = append(summary.Errors, fmt.Sprintf("purging target directory %s: %v", targetName, err))
			_ = syscall.Flock(int(tombF.Fd()), syscall.LOCK_UN)
			tombF.Close()
			continue
		}
		_ = unix.Fsync(rootFd)

		// Unlink tombstone after directory removal
		if err := unix.Unlinkat(tombDirFd, tombstoneID+".tombstone", 0); err != nil {
			summary.Errors = append(summary.Errors, fmt.Sprintf("unlinking tombstone %s: %v", tombstoneID, err))
		} else {
			_ = unix.Fsync(tombDirFd)
			summary.Removed++
		}

		_ = syscall.Flock(int(tombF.Fd()), syscall.LOCK_UN)
		tombF.Close()
	}

	return summary, nil
}
