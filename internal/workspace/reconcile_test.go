package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
	"github.com/google/uuid"
)

type mockStore struct {
	mu     sync.Mutex
	runs   map[string]*drevtypes.Run
	getErr map[string]error
}

func newMockStore() *mockStore {
	return &mockStore{
		runs:   make(map[string]*drevtypes.Run),
		getErr: make(map[string]error),
	}
}

func (m *mockStore) setRun(run *drevtypes.Run) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.ID] = run
}

func (m *mockStore) setErr(runID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getErr[runID] = err
}

func (m *mockStore) GetRun(ctx context.Context, id string) (*drevtypes.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.getErr[id]; ok {
		return nil, err
	}
	if run, ok := m.runs[id]; ok {
		return run, nil
	}
	return nil, errors.New("run not found")
}

func TestCleanup_DatabaseUnavailablePreservesWorkspace(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-db-unavailable"

	// Configure DB failure
	store.setErr(runID, errors.New("database connection refused"))

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}

	// Attempt cleanup
	err = ws.Cleanup()
	if err == nil {
		t.Fatalf("expected Cleanup() to return error when DB is unavailable, got nil")
	}

	// Workspace directory MUST still exist
	if _, err := os.Stat(ws.Dir); os.IsNotExist(err) {
		t.Fatalf("workspace directory was deleted despite database check failure!")
	}
}

func TestCleanup_NonTerminalStatusPreservesWorkspace(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-still-running"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusRunning,
	})

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}

	err = ws.Cleanup()
	if err == nil {
		t.Fatalf("expected Cleanup() to return error when run is not terminal, got nil")
	}

	// Workspace directory MUST still exist
	if _, err := os.Stat(ws.Dir); os.IsNotExist(err) {
		t.Fatalf("workspace directory was deleted while run was still running!")
	}
}

func TestCleanup_PanicWhileRunningPreservesWorkspace(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-panic-test"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusRunning,
	})

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}

	// Simulate panic during execution where deferred cleanup is invoked
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic to be recovered")
		}

		// Workspace must still exist because terminal state was not persisted
		if _, err := os.Stat(ws.Dir); os.IsNotExist(err) {
			t.Fatalf("workspace was deleted during panic unwinding!")
		}
	}()

	func() {
		defer ws.Cleanup()
		panic("fatal unexpected runner crash")
	}()
}

func TestCleanup_TerminalStatusCleansWorkspace(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-success-cleanup"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusSuccess,
	})

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}

	err = ws.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup() failed unexpectedly: %v", err)
	}

	// Workspace must be gone
	if _, err := os.Stat(ws.Dir); !os.IsNotExist(err) {
		t.Fatalf("workspace directory should be deleted, but still exists: %s", ws.Dir)
	}
}

func TestReconcile_ActiveWorkspacePreserved(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-active-lock"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusSuccess,
	})

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}
	defer ws.lockFile.Close() // Keep lock held during reconciliation test

	summary, err := ReconcileOrphans(context.Background(), rootDir, store, nil)
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 0 {
		t.Fatalf("expected 0 removed workspaces, got %d", summary.Removed)
	}
	if summary.Skipped != 1 {
		t.Fatalf("expected 1 skipped workspace, got %d", summary.Skipped)
	}

	// Workspace must still exist
	if _, err := os.Stat(ws.Dir); os.IsNotExist(err) {
		t.Fatalf("active workspace was deleted while locked!")
	}
}

func TestReconcile_OrphanedTerminalWorkspaceRemoved(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-orphaned-dead"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusFailed,
	})

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}

	// Simulate daemon death / SIGKILL by closing lock file descriptor without running Cleanup
	_ = ws.lockFile.Close()

	summary, err := ReconcileOrphans(context.Background(), rootDir, store, nil)
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 1 {
		t.Fatalf("expected 1 removed workspace, got %d (errors: %v)", summary.Removed, summary.Errors)
	}

	// Workspace directory should be completely removed
	if _, err := os.Stat(ws.Dir); !os.IsNotExist(err) {
		t.Fatalf("orphaned workspace was not removed: %s", ws.Dir)
	}
}

func TestReconcile_TombstoneRecoveryAfterCrash(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()
	runID := "run-crashed-during-deletion"

	store.setRun(&drevtypes.Run{
		ID:     runID,
		Status: drevtypes.StatusFailed,
	})

	// Set up an orphaned tombstone representing a crash mid-deletion
	tombstoneDir := filepath.Join(rootDir, ".tombstones")
	if err := os.MkdirAll(tombstoneDir, 0700); err != nil {
		t.Fatal(err)
	}

	tombID := uuid.New().String()
	originalDirName := fmt.Sprintf("drev-workspace-%s-abcd1234", runID)
	targetDirName := fmt.Sprintf("drev-deleting-%s-abcd1234", runID)
	targetDirPath := filepath.Join(rootDir, targetDirName)

	if err := os.Mkdir(targetDirPath, 0700); err != nil {
		t.Fatal(err)
	}
	leftoverFile := filepath.Join(targetDirPath, "leftover.log")
	if err := os.WriteFile(leftoverFile, []byte("interrupted data"), 0600); err != nil {
		t.Fatal(err)
	}

	tombstonePath := filepath.Join(tombstoneDir, tombID+".tombstone")
	ts := Tombstone{
		Magic:          TombstoneMagic,
		SchemaVersion:  TombstoneVersion,
		TombstoneID:    tombID,
		RunID:          runID,
		OriginalDir:    originalDirName,
		TargetDir:      targetDirName,
		DeletionIntent: DeletionIntentCleanup,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}

	f, err := PublishTombstone(rootDir, ts)
	if err != nil {
		t.Fatalf("PublishTombstone failed: %v", err)
	}
	// Simulate daemon exit right after tombstone publish and rename: close lock file
	_ = f.Close()

	// Run reconciliation Pass 1
	summary, err := ReconcileOrphans(context.Background(), rootDir, store, nil)
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 1 {
		t.Fatalf("expected 1 removed remnant, got %d (errors: %v)", summary.Removed, summary.Errors)
	}

	// Verify targetDir is removed
	if _, err := os.Stat(targetDirPath); !os.IsNotExist(err) {
		t.Fatalf("target directory was not removed by tombstone recovery")
	}

	// Verify tombstone file is unlinked
	if _, err := os.Stat(tombstonePath); !os.IsNotExist(err) {
		t.Fatalf("tombstone file was not unlinked after successful recovery")
	}
}

func TestReconcile_UntrackedRemnantSkipped(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()

	// Create an unverified/untracked remnant directory
	remnantDir := filepath.Join(rootDir, "drev-deleting-orphan-no-tombstone")
	if err := os.Mkdir(remnantDir, 0700); err != nil {
		t.Fatal(err)
	}

	summary, err := ReconcileOrphans(context.Background(), rootDir, store, nil)
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 0 {
		t.Fatalf("untracked remnant should NOT be removed, but removed count is %d", summary.Removed)
	}
	if summary.Skipped != 1 {
		t.Fatalf("expected 1 skipped remnant, got %d", summary.Skipped)
	}

	// Remnant must be preserved
	if _, err := os.Stat(remnantDir); err != nil {
		t.Fatalf("untracked remnant directory was deleted: %v", err)
	}
}

func TestReconcile_CorruptTombstoneSkipped(t *testing.T) {
	rootDir := t.TempDir()
	store := newMockStore()

	tombstoneDir := filepath.Join(rootDir, ".tombstones")
	if err := os.MkdirAll(tombstoneDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Write garbage to a tombstone file
	tombstonePath := filepath.Join(tombstoneDir, "corrupted.tombstone")
	if err := os.WriteFile(tombstonePath, []byte("NOT-JSON-GARBAGE"), 0600); err != nil {
		t.Fatal(err)
	}

	summary, err := ReconcileOrphans(context.Background(), rootDir, store, nil)
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 0 {
		t.Fatalf("expected 0 removed, got %d", summary.Removed)
	}
	if summary.Skipped != 1 {
		t.Fatalf("expected 1 skipped, got %d", summary.Skipped)
	}
	if len(summary.Errors) == 0 {
		t.Fatalf("expected errors to record corrupted tombstone")
	}

	// Corrupted tombstone must not be unlinked
	if _, err := os.Stat(tombstonePath); err != nil {
		t.Fatalf("corrupted tombstone should not have been unlinked: %v", err)
	}
}

func TestReconcile_InterruptedRunIDsAuthorizesCleanup(t *testing.T) {
	rootDir := t.TempDir()
	// Store is empty - run is NOT in the store
	store := newMockStore()
	runID := "ghost-run-12345"

	ws, err := CreateForRun(rootDir, runID, store)
	if err != nil {
		t.Fatalf("CreateForRun failed: %v", err)
	}
	_ = ws.lockFile.Close() // Simulate daemon crash

	// Reconciliation is provided interruptedRunIDs containing this ghost run
	summary, err := ReconcileOrphans(context.Background(), rootDir, store, []string{runID})
	if err != nil {
		t.Fatalf("ReconcileOrphans failed: %v", err)
	}

	if summary.Removed != 1 {
		t.Fatalf("expected ghost run to be authorized and removed via interruptedRunIDs, got removed=%d (errors: %v)", summary.Removed, summary.Errors)
	}

	if _, err := os.Stat(ws.Dir); !os.IsNotExist(err) {
		t.Fatalf("workspace should be removed after ghost run reconciliation")
	}
}

