package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	TombstoneMagic        = "drev-ci-tombstone"
	TombstoneVersion      = 1
	DeletionIntentCleanup = "confirmed_cleanup"
	WorkspaceMetaMagic    = "drev-ci-workspace"
	WorkspaceMetaVersion  = 1
)

// Tombstone represents an on-disk deletion authorization ledger entry.
type Tombstone struct {
	Magic          string `json:"magic"`
	SchemaVersion  int    `json:"schema_version"`
	TombstoneID    string `json:"tombstone_id"`
	RunID          string `json:"run_id"`
	OriginalDir    string `json:"original_dir"`
	TargetDir      string `json:"target_dir"`
	DeletionIntent string `json:"deletion_intent"`
	CreatedAt      string `json:"created_at"`
	CreatorPID     int    `json:"creator_pid"`
}

// WorkspaceMetadata represents provenance stored in .drev_meta.json inside workspaces.
type WorkspaceMetadata struct {
	Magic         string `json:"magic"`
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	OriginalDir   string `json:"original_dir"`
	CreatedAt     string `json:"created_at"`
	CreatorPID    int    `json:"creator_pid"`
}

// ValidateBasename validates that name is a flat basename matching expectedPrefix without path separators.
func ValidateBasename(name, expectedPrefix string) error {
	if name == "" {
		return errors.New("directory name cannot be empty")
	}
	if strings.ContainsAny(name, "/\\:\x00") {
		return fmt.Errorf("directory name %q contains unsafe characters", name)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid directory name %q", name)
	}
	if filepath.Base(name) != name || filepath.Clean(name) != name {
		return fmt.Errorf("directory name %q is not a clean flat basename", name)
	}
	if !strings.HasPrefix(name, expectedPrefix) {
		return fmt.Errorf("directory name %q does not match required prefix %q", name, expectedPrefix)
	}
	return nil
}

// ValidateTombstone verifies all structural and intent invariants of a tombstone record.
func ValidateTombstone(ts *Tombstone, fileName string) error {
	if ts.Magic != TombstoneMagic {
		return fmt.Errorf("invalid tombstone magic %q", ts.Magic)
	}
	if ts.SchemaVersion != TombstoneVersion {
		return fmt.Errorf("unsupported tombstone schema version %d", ts.SchemaVersion)
	}
	if ts.DeletionIntent != DeletionIntentCleanup {
		return fmt.Errorf("unauthorized deletion intent %q", ts.DeletionIntent)
	}
	if ts.TombstoneID == "" {
		return errors.New("empty tombstone ID")
	}
	if fileName != "" && fileName != ts.TombstoneID+".tombstone" {
		return fmt.Errorf("tombstone filename %q does not match tombstone ID %q", fileName, ts.TombstoneID)
	}
	if ts.RunID == "" || strings.ContainsAny(ts.RunID, "/\\:\x00") {
		return fmt.Errorf("invalid run ID %q in tombstone", ts.RunID)
	}
	if err := ValidateBasename(ts.OriginalDir, "drev-workspace-"); err != nil {
		return fmt.Errorf("invalid original_dir: %w", err)
	}
	if err := ValidateBasename(ts.TargetDir, "drev-deleting-"); err != nil {
		return fmt.Errorf("invalid target_dir: %w", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, ts.CreatedAt); err != nil {
		if _, err2 := time.Parse(time.RFC3339, ts.CreatedAt); err2 != nil {
			return fmt.Errorf("invalid created_at timestamp %q", ts.CreatedAt)
		}
	}
	return nil
}

// PublishTombstone publishes a tombstone file atomically without replacement,
// keeping the descriptor open and locked throughout subsequent deletion.
func PublishTombstone(wsRoot string, ts Tombstone) (*os.File, error) {
	tombstoneDir := filepath.Join(wsRoot, ".tombstones")
	if err := os.MkdirAll(tombstoneDir, 0700); err != nil {
		return nil, fmt.Errorf("creating tombstone directory: %w", err)
	}

	tombDirFd, err := unix.Open(tombstoneDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, fmt.Errorf("opening tombstone directory descriptor: %w", err)
	}
	defer unix.Close(tombDirFd)

	tmpName := fmt.Sprintf(".tmp-%s-%d.tombstone", ts.TombstoneID, time.Now().UnixNano())
	tmpPath := filepath.Join(tombstoneDir, tmpName)
	finalName := ts.TombstoneID + ".tombstone"
	finalPath := filepath.Join(tombstoneDir, finalName)

	tmpF, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("creating temporary tombstone file: %w", err)
	}

	// Lock descriptor immediately
	if err := syscall.Flock(int(tmpF.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = tmpF.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("locking temporary tombstone: %w", err)
	}

	payload, err := json.MarshalIndent(ts, "", "  ")
	if err != nil {
		_ = syscall.Flock(int(tmpF.Fd()), syscall.LOCK_UN)
		_ = tmpF.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("serializing tombstone: %w", err)
	}

	n, err := tmpF.Write(payload)
	if err != nil || n != len(payload) {
		_ = syscall.Flock(int(tmpF.Fd()), syscall.LOCK_UN)
		_ = tmpF.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing tombstone payload: %w", err)
	}

	if err := tmpF.Sync(); err != nil {
		_ = syscall.Flock(int(tmpF.Fd()), syscall.LOCK_UN)
		_ = tmpF.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("syncing temporary tombstone: %w", err)
	}

	// Atomic no-replace transition to final publication name
	if err := renameNoReplaceAt(tombDirFd, tmpName, tombDirFd, finalName); err != nil {
		_ = syscall.Flock(int(tmpF.Fd()), syscall.LOCK_UN)
		_ = tmpF.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("publishing tombstone without replacement: %w", err)
	}

	// Flush directory descriptor to persist directory entry to physical media
	if err := unix.Fsync(tombDirFd); err != nil {
		_ = syscall.Flock(int(tmpF.Fd()), syscall.LOCK_UN)
		_ = tmpF.Close()
		_ = os.Remove(finalPath)
		return nil, fmt.Errorf("syncing tombstone directory: %w", err)
	}

	return tmpF, nil
}
