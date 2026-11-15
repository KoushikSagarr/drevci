//go:build linux

package workspace

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// renameNoReplaceAt atomically renames oldName to newName relative to parent file descriptors,
// failing with syscall.EEXIST if newName already exists.
func renameNoReplaceAt(fromDirFd int, fromName string, toDirFd int, toName string) error {
	err := unix.Renameat2(fromDirFd, fromName, toDirFd, toName, unix.RENAME_NOREPLACE)
	if err != nil {
		return fmt.Errorf("renameat2 RENAME_NOREPLACE (%s -> %s): %w", fromName, toName, err)
	}
	return nil
}
