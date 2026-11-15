//go:build !darwin && !linux

package workspace

import (
	"errors"
)

var ErrUnsupportedPlatform = errors.New("atomic no-replace directory rename is not supported on this platform; refusing to execute unsafe rename")

// renameNoReplaceAt fails closed on platforms without a verified atomic no-replace primitive.
func renameNoReplaceAt(fromDirFd int, fromName string, toDirFd int, toName string) error {
	return ErrUnsupportedPlatform
}
