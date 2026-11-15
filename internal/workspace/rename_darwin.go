//go:build darwin

package workspace

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// renameNoReplaceAt atomically renames oldName to newName relative to parent file descriptors,
// failing with syscall.EEXIST if newName already exists.
func renameNoReplaceAt(fromDirFd int, fromName string, toDirFd int, toName string) error {
	err := unix.RenameatxNp(fromDirFd, fromName, toDirFd, toName, unix.RENAME_EXCL)
	if err != nil {
		return fmt.Errorf("renameatx_np RENAME_EXCL (%s -> %s): %w", fromName, toName, err)
	}
	return nil
}
