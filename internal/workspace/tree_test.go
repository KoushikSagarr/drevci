package workspace

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestTree_SymlinkOutsideWorkspaceUntouched(t *testing.T) {
	tmp := t.TempDir()

	// External directory outside the workspace
	extDir := filepath.Join(tmp, "external")
	if err := os.Mkdir(extDir, 0755); err != nil {
		t.Fatal(err)
	}
	extFile := filepath.Join(extDir, "secret.txt")
	extContent := []byte("confidential-data")
	if err := os.WriteFile(extFile, extContent, 0644); err != nil {
		t.Fatal(err)
	}

	initialExtFileStat, err := os.Stat(extFile)
	if err != nil {
		t.Fatal(err)
	}

	// Workspace directory
	wsDir := filepath.Join(tmp, "workspace")
	if err := os.Mkdir(wsDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Symlink to external file
	symlinkToFile := filepath.Join(wsDir, "link-to-file")
	if err := os.Symlink(extFile, symlinkToFile); err != nil {
		t.Fatal(err)
	}

	// Symlink to external dir
	symlinkToDir := filepath.Join(wsDir, "link-to-dir")
	if err := os.Symlink(extDir, symlinkToDir); err != nil {
		t.Fatal(err)
	}

	// Subdirectory inside workspace containing relative symlink out
	wsSub := filepath.Join(wsDir, "sub")
	if err := os.Mkdir(wsSub, 0700); err != nil {
		t.Fatal(err)
	}
	symlinkRel := filepath.Join(wsSub, "rel-link")
	if err := os.Symlink(filepath.Join("..", "..", "external", "secret.txt"), symlinkRel); err != nil {
		t.Fatal(err)
	}

	wsFd, err := unix.Open(wsDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := removeTreeAt(wsFd); err != nil {
		_ = unix.Close(wsFd)
		t.Fatalf("removeTreeAt failed: %v", err)
	}
	_ = unix.Close(wsFd)

	// Verify external directory and file are completely untouched
	currentExtContent, err := os.ReadFile(extFile)
	if err != nil {
		t.Fatalf("external file was destroyed or unreadable: %v", err)
	}
	if string(currentExtContent) != string(extContent) {
		t.Fatalf("external file content modified! expected %q, got %q", extContent, currentExtContent)
	}

	currentExtFileStat, err := os.Stat(extFile)
	if err != nil {
		t.Fatal(err)
	}
	if currentExtFileStat.Mode() != initialExtFileStat.Mode() {
		t.Fatalf("external file permissions were modified! expected %v, got %v", initialExtFileStat.Mode(), currentExtFileStat.Mode())
	}
}

func TestTree_ReadOnlyFilesAndDirectories(t *testing.T) {
	tmp := t.TempDir()

	wsDir := filepath.Join(tmp, "readonly-ws")
	if err := os.Mkdir(wsDir, 0700); err != nil {
		t.Fatal(err)
	}

	subDir := filepath.Join(wsDir, "sub")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	roFile := filepath.Join(subDir, "locked.txt")
	if err := os.WriteFile(roFile, []byte("readonly"), 0400); err != nil {
		t.Fatal(err)
	}

	// Make subdirectory read-only (no write permission to directory entries)
	if err := os.Chmod(subDir, 0500); err != nil {
		t.Fatal(err)
	}

	wsFd, err := unix.Open(wsDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := removeTreeAt(wsFd); err != nil {
		_ = unix.Close(wsFd)
		t.Fatalf("removeTreeAt failed on read-only structure: %v", err)
	}
	_ = unix.Close(wsFd)

	// Ensure subDir and its contents are gone
	entries, err := os.ReadDir(wsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("expected workspace to be empty, found %d entries", len(entries))
	}
}

func TestTree_RenameNoReplace(t *testing.T) {
	tmp := t.TempDir()

	dirA := filepath.Join(tmp, "dirA")
	dirB := filepath.Join(tmp, "dirB")
	dirC := filepath.Join(tmp, "dirC")

	if err := os.Mkdir(dirA, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dirB, 0700); err != nil {
		t.Fatal(err)
	}

	rootFd, err := unix.Open(tmp, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(rootFd)

	// dirA -> dirB should fail with collision because dirB already exists
	err = renameNoReplaceAt(rootFd, "dirA", rootFd, "dirB")
	if err == nil {
		t.Fatalf("expected renameNoReplaceAt to fail on existing target, but succeeded")
	}

	// Both dirA and dirB must still exist
	if _, err := os.Stat(dirA); err != nil {
		t.Fatalf("dirA should still exist: %v", err)
	}
	if _, err := os.Stat(dirB); err != nil {
		t.Fatalf("dirB should still exist: %v", err)
	}

	// dirA -> dirC should succeed because dirC does not exist
	err = renameNoReplaceAt(rootFd, "dirA", rootFd, "dirC")
	if err != nil {
		t.Fatalf("expected renameNoReplaceAt to succeed for non-existent target: %v", err)
	}

	if _, err := os.Stat(dirA); !os.IsNotExist(err) {
		t.Fatalf("dirA should no longer exist after successful rename")
	}
	if _, err := os.Stat(dirC); err != nil {
		t.Fatalf("dirC should exist after successful rename: %v", err)
	}
}
