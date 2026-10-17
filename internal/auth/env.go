package auth

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PersistEnvSecrets writes or updates the provided key-value pairs in the specified .env file.
// It preserves all existing unrelated environment variables, comments, and empty lines.
// It writes with restrictive 0600 permissions and avoids exposing secret values in error messages.
func PersistEnvSecrets(envPath string, updates map[string]string) error {
	if len(updates) == 0 {
		return nil
	}

	var lines []string
	foundKeys := make(map[string]bool)

	content, err := os.ReadFile(envPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reading env file: %w", err)
	}

	if err == nil {
		rawLines := strings.Split(string(content), "\n")
		// If the file ends with a trailing empty string from split, drop it for line processing
		if len(rawLines) > 0 && rawLines[len(rawLines)-1] == "" {
			rawLines = rawLines[:len(rawLines)-1]
		}

		for _, line := range rawLines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") || !strings.Contains(line, "=") {
				lines = append(lines, line)
				continue
			}

			eqIdx := strings.Index(line, "=")
			key := strings.TrimSpace(line[:eqIdx])
			if newVal, ok := updates[key]; ok {
				lines = append(lines, fmt.Sprintf("%s=%s", key, newVal))
				foundKeys[key] = true
			} else {
				lines = append(lines, line)
			}
		}
	}

	// Append any updates that were not already present
	for key, val := range updates {
		if !foundKeys[key] {
			lines = append(lines, fmt.Sprintf("%s=%s", key, val))
		}
	}

	// Reconstruct file with trailing newline
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l)
		buf.WriteString("\n")
	}

	dir := filepath.Dir(envPath)
	tmpFile, err := os.CreateTemp(dir, ".env.tmp*")
	if err != nil {
		// Fallback to direct write if temp file creation in dir fails
		if writeErr := os.WriteFile(envPath, buf.Bytes(), 0600); writeErr != nil {
			return fmt.Errorf("writing env file: %w", writeErr)
		}
		_ = os.Chmod(envPath, 0600)
		return nil
	}

	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if err := tmpFile.Chmod(0600); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("setting env file permissions: %w", err)
	}

	if _, err := tmpFile.Write(buf.Bytes()); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("writing env file data: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("syncing env file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing env file: %w", err)
	}

	if err := os.Rename(tmpName, envPath); err != nil {
		// If rename fails (e.g. cross-filesystem), fallback to direct write
		if writeErr := os.WriteFile(envPath, buf.Bytes(), 0600); writeErr != nil {
			return fmt.Errorf("replacing env file: %w", writeErr)
		}
	}

	_ = os.Chmod(envPath, 0600)
	return nil
}
