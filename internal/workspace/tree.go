package workspace

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// removeTreeAt recursively removes all entries inside dirFd descriptor-relatively.
// It never follows symlinks and never modifies permissions on untrusted files or symlinks.
// Directory permissions are changed exclusively through fchmod on the verified opened descriptor.
func removeTreeAt(dirFd int) error {
	dupFd, err := unix.Dup(dirFd)
	if err != nil {
		return fmt.Errorf("duplicating directory descriptor: %w", err)
	}
	f := os.NewFile(uintptr(dupFd), "")
	defer f.Close()

	entries, err := f.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("reading directory entries: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()

		var stat unix.Stat_t
		if err := unix.Fstatat(dirFd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
				continue
			}
			return fmt.Errorf("fstatat %s: %w", name, err)
		}

		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			// Open child directory strictly with O_DIRECTORY | O_NOFOLLOW
			childFd, err := unix.Openat(dirFd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
			if err != nil {
				return fmt.Errorf("openat directory %s (O_NOFOLLOW): %w", name, err)
			}

			// Safely change permissions through fchmod on the opened descriptor (never touches symlink targets)
			if err := unix.Fchmod(childFd, 0700); err != nil {
				_ = unix.Close(childFd)
				return fmt.Errorf("fchmod child directory %s: %w", name, err)
			}

			if err := removeTreeAt(childFd); err != nil {
				_ = unix.Close(childFd)
				return err
			}
			_ = unix.Close(childFd)

			if err := unix.Unlinkat(dirFd, name, unix.AT_REMOVEDIR); err != nil {
				return fmt.Errorf("unlinkat rmdir %s: %w", name, err)
			}
		} else {
			// Regular file, symlink, fifo, socket: unlink directly without modifying permissions
			if err := unix.Unlinkat(dirFd, name, 0); err != nil {
				return fmt.Errorf("unlinkat file %s: %w", name, err)
			}
		}
	}

	return nil
}

// removeDirectoryUnderRoot removes a direct-child directory of rootFd descriptor-relatively.
func removeDirectoryUnderRoot(rootFd int, dirName string) error {
	targetFd, err := unix.Openat(rootFd, dirName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return fmt.Errorf("openat target directory %s: %w", dirName, err)
	}

	_ = unix.Fchmod(targetFd, 0700)

	if err := removeTreeAt(targetFd); err != nil {
		_ = unix.Close(targetFd)
		return err
	}
	_ = unix.Close(targetFd)

	if err := unix.Unlinkat(rootFd, dirName, unix.AT_REMOVEDIR); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return fmt.Errorf("unlinkat rmdir target %s: %w", dirName, err)
	}

	return nil
}
