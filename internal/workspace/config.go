package workspace

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/pkg/drevtypes"
)

// ErrConfigNotFound indicates that .drev.yml is genuinely absent from the repository root.
// This is the only error condition under which host fallback is permissible.
var ErrConfigNotFound = errors.New(".drev.yml not found in repository root")

const configFilename = ".drev.yml"

// LoadRepositoryConfig inspects, canonicalizes, resolves symlinks, and parses .drev.yml
// from the checked-out workspace source directory (w.SourceDir()).
// It guarantees that the parsed pipeline's Source metadata is set to triggerSource,
// ignoring any source metadata declared inside the repository .drev.yml file.
//
// Return contracts:
// - If .drev.yml is genuinely absent: returns (nil, ErrConfigNotFound).
// - If .drev.yml exists and is valid: returns (*drevtypes.Pipeline, nil) with pipeline.Source = triggerSource.
// - If .drev.yml exists but is malformed, invalid, unreadable, dangling, or escapes the workspace:
//   returns (nil, err) where !errors.Is(err, ErrConfigNotFound).
func (w *Workspace) LoadRepositoryConfig(triggerSource drevtypes.Source) (*drevtypes.Pipeline, error) {
	sourceDir := w.SourceDir()

	// 1. Canonicalize the workspace source directory root.
	canonicalRoot, err := filepath.EvalSymlinks(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("canonicalizing workspace source directory: %w", err)
	}
	canonicalRoot = filepath.Clean(canonicalRoot)

	// 2. Candidate configuration path at repository root.
	candidatePath := filepath.Join(sourceDir, configFilename)

	// 3. Inspect candidate with os.Lstat before symlink resolution.
	// Only os.IsNotExist on this initial lstat qualifies as genuine absence (ErrConfigNotFound).
	_, err = os.Lstat(candidatePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrConfigNotFound
		}
		return nil, fmt.Errorf("inspecting %s: %w", configFilename, err)
	}

	// 4. Resolve symlinks to obtain the canonical target path.
	// If the file is a dangling symlink or loop, EvalSymlinks will fail.
	canonicalTarget, err := filepath.EvalSymlinks(candidatePath)
	if err != nil {
		return nil, fmt.Errorf("resolving %s symlink: %w", configFilename, err)
	}
	canonicalTarget = filepath.Clean(canonicalTarget)

	// 5. Containment check: verify canonicalTarget is within canonicalRoot.
	rel, err := filepath.Rel(canonicalRoot, canonicalTarget)
	if err != nil {
		return nil, fmt.Errorf("evaluating relative path for %s: %w", configFilename, err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("configuration file %s escapes workspace root: %s", configFilename, canonicalTarget)
	}
	if !strings.HasPrefix(canonicalTarget, canonicalRoot+string(filepath.Separator)) {
		return nil, fmt.Errorf("configuration file %s escapes workspace root: %s", configFilename, canonicalTarget)
	}

	// 6. Ensure the target is a regular file (reject directories, sockets, devices, FIFOs).
	fi, err := os.Stat(canonicalTarget)
	if err != nil {
		return nil, fmt.Errorf("verifying %s: %w", configFilename, err)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("configuration file %s is a directory", configFilename)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("configuration file %s is not a regular file (mode: %v)", configFilename, fi.Mode())
	}

	// 7. Read and parse configuration.
	data, err := os.ReadFile(canonicalTarget)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", configFilename, err)
	}

	pipeline, err := parser.ParseBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", configFilename, err)
	}

	// 8. Authoritative trigger Source metadata unconditionally replaces any in-repo source definition.
	pipeline.Source = triggerSource

	return pipeline, nil
}

// ResolveConfig loads repository configuration with an optional, separately supplied host-side fallback.
//
// Return contracts:
// - If repository configuration exists and is valid: returns the repository pipeline (authoritative).
// - If repository configuration is genuinely absent (errors.Is(err, ErrConfigNotFound)):
//     - If hostFallback != nil and len(hostFallback.Jobs) > 0:
//         emits a deprecation warning to logWriter, shallow-copies hostFallback, applies triggerSource, and returns it.
//     - Else: returns an error indicating no configuration was found.
// - If any other error occurs (malformed, invalid, unreadable, dangling, or escaping repository configuration):
//     returns the error immediately without falling back.
func (w *Workspace) ResolveConfig(
	triggerSource drevtypes.Source,
	hostFallback *drevtypes.Pipeline,
	logWriter io.Writer,
) (*drevtypes.Pipeline, error) {
	repoPipeline, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		return repoPipeline, nil
	}

	// Fallback is ONLY permitted when .drev.yml is genuinely absent.
	if errors.Is(err, ErrConfigNotFound) {
		if hostFallback != nil && len(hostFallback.Jobs) > 0 {
			if logWriter != nil {
				fmt.Fprint(logWriter, "[drev] warning: no .drev.yml found in repository root; falling back to host configuration (deprecated)\n")
			}
			// Shallow copy to prevent mutating the caller's fallback object or leaking state across runs.
			cp := *hostFallback
			cp.Source = triggerSource
			return &cp, nil
		}
		return nil, fmt.Errorf("no %s found in repository and no host fallback configured: %w", configFilename, ErrConfigNotFound)
	}

	// For any hard error (syntax error, validation error, symlink escape, dangling link, unreadable),
	// fail immediately without falling back.
	return nil, err
}
