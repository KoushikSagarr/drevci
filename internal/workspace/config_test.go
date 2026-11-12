package workspace

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

func setupTestWorkspace(t *testing.T) *Workspace {
	t.Helper()
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() failed: %v", err)
	}
	t.Cleanup(func() {
		_ = w.Cleanup()
	})
	if err := os.MkdirAll(w.SourceDir(), 0755); err != nil {
		t.Fatalf("creating source dir: %v", err)
	}
	return w
}

func sampleTriggerSource() drevtypes.Source {
	return drevtypes.Source{
		Type:      "git",
		URL:       "https://github.com/octocat/hello-world.git",
		Ref:       "refs/pull/42/head",
		CommitSHA: "b1b2c3d4e5f678901234567890abcdef12345678",
		PRNumber:  42,
	}
}

func sampleValidPipelineYAML() string {
	return `
name: repo-pipeline
jobs:
  - name: test-job
    image: alpine:latest
    steps:
      - name: step-1
        run: echo ok
`
}

func sampleHostFallback() *drevtypes.Pipeline {
	return &drevtypes.Pipeline{
		Name: "fallback-pipeline",
		Jobs: []drevtypes.Job{
			{
				Name:  "fallback-job",
				Image: "alpine:3.19",
				Steps: []drevtypes.Step{
					{Name: "step-fb", Run: "echo fallback"},
				},
			},
		},
	}
}

func TestLoadRepositoryConfig_Success(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(sampleValidPipelineYAML()), 0644); err != nil {
		t.Fatalf("writing .drev.yml: %v", err)
	}

	p, err := w.LoadRepositoryConfig(triggerSource)
	if err != nil {
		t.Fatalf("LoadRepositoryConfig failed: %v", err)
	}
	if p.Name != "repo-pipeline" {
		t.Errorf("expected pipeline name %q, got %q", "repo-pipeline", p.Name)
	}
	if len(p.Jobs) != 1 || p.Jobs[0].Name != "test-job" {
		t.Errorf("expected 1 job 'test-job', got %v", p.Jobs)
	}
	if p.Source != triggerSource {
		t.Errorf("expected Source %+v, got %+v", triggerSource, p.Source)
	}
}

func TestLoadRepositoryConfig_PreservesAuthoritativeSource_IgnoringInRepoSource(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// In-repo config declares an untrusted source section trying to forge revision metadata
	inRepoYAML := `
name: repo-pipeline
source:
  type: svn
  url: https://evil.com/evil.git
  ref: refs/heads/evil-branch
  commit_sha: 0000000000000000000000000000000000000000
  pr_number: 9999
jobs:
  - name: test-job
    image: alpine:latest
    steps:
      - name: s1
        run: echo secure
`
	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(inRepoYAML), 0644); err != nil {
		t.Fatalf("writing .drev.yml: %v", err)
	}

	p, err := w.LoadRepositoryConfig(triggerSource)
	if err != nil {
		t.Fatalf("LoadRepositoryConfig failed: %v", err)
	}

	// Verify all 5 trigger Source fields survive unchanged
	if p.Source.Type != triggerSource.Type {
		t.Errorf("expected Source.Type %q, got %q", triggerSource.Type, p.Source.Type)
	}
	if p.Source.URL != triggerSource.URL {
		t.Errorf("expected Source.URL %q, got %q", triggerSource.URL, p.Source.URL)
	}
	if p.Source.Ref != triggerSource.Ref {
		t.Errorf("expected Source.Ref %q, got %q", triggerSource.Ref, p.Source.Ref)
	}
	if p.Source.CommitSHA != triggerSource.CommitSHA {
		t.Errorf("expected Source.CommitSHA %q, got %q", triggerSource.CommitSHA, p.Source.CommitSHA)
	}
	if p.Source.PRNumber != triggerSource.PRNumber {
		t.Errorf("expected Source.PRNumber %d, got %d", triggerSource.PRNumber, p.Source.PRNumber)
	}
}

func TestLoadRepositoryConfig_GenuinelyAbsent(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// No .drev.yml created
	p, err := w.LoadRepositoryConfig(triggerSource)
	if p != nil {
		t.Errorf("expected nil pipeline, got %+v", p)
	}
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("expected ErrConfigNotFound, got: %v", err)
	}
}

func TestLoadRepositoryConfig_InternalSymlink_Accepted(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	subDir := filepath.Join(w.SourceDir(), "ci", "configs")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("creating subdir: %v", err)
	}

	realConfig := filepath.Join(subDir, "pipeline.yml")
	if err := os.WriteFile(realConfig, []byte(sampleValidPipelineYAML()), 0644); err != nil {
		t.Fatalf("writing real config: %v", err)
	}

	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink(filepath.Join("ci", "configs", "pipeline.yml"), symlinkPath); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	p, err := w.LoadRepositoryConfig(triggerSource)
	if err != nil {
		t.Fatalf("LoadRepositoryConfig failed on internal symlink: %v", err)
	}
	if p.Name != "repo-pipeline" {
		t.Errorf("expected pipeline name %q, got %q", "repo-pipeline", p.Name)
	}
	if p.Source != triggerSource {
		t.Errorf("expected Source %+v, got %+v", triggerSource, p.Source)
	}
}

func TestLoadRepositoryConfig_ExternalSymlink_Rejected(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// External file outside workspace
	externalDir := t.TempDir()
	externalConfig := filepath.Join(externalDir, "external.drev.yml")
	if err := os.WriteFile(externalConfig, []byte(sampleValidPipelineYAML()), 0644); err != nil {
		t.Fatalf("writing external config: %v", err)
	}

	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink(externalConfig, symlinkPath); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	_, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		t.Fatal("expected error on external symlink, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("external symlink must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if !strings.Contains(err.Error(), "escapes workspace root") {
		t.Errorf("expected error message to contain 'escapes workspace root', got: %v", err)
	}
}

func TestLoadRepositoryConfig_RelativeTraversalSymlink_Rejected(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// Symlink pointing through relative traversal outside the workspace
	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink(filepath.Join("..", "..", "outside.yml"), symlinkPath); err != nil {
		t.Fatalf("creating traversal symlink: %v", err)
	}

	_, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		t.Fatal("expected error on traversal symlink, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("traversal symlink must not be reported as ErrConfigNotFound, got: %v", err)
	}
}

func TestLoadRepositoryConfig_DanglingSymlink_FailsSafely(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// Symlink pointing to non-existent target
	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink("nonexistent-target.yml", symlinkPath); err != nil {
		t.Fatalf("creating dangling symlink: %v", err)
	}

	_, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		t.Fatal("expected error on dangling symlink, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("dangling symlink must not be treated as ErrConfigNotFound, got: %v", err)
	}
	if !strings.Contains(err.Error(), "resolving .drev.yml symlink") {
		t.Errorf("expected error to indicate symlink resolution failure, got: %v", err)
	}
}

func TestLoadRepositoryConfig_Directory_Rejected(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	dirConfig := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Mkdir(dirConfig, 0755); err != nil {
		t.Fatalf("creating directory .drev.yml: %v", err)
	}

	_, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		t.Fatal("expected error when .drev.yml is a directory, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("directory .drev.yml must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if !strings.Contains(err.Error(), "is a directory") {
		t.Errorf("expected error message to contain 'is a directory', got: %v", err)
	}
}

func TestLoadRepositoryConfig_UnreadableFile(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(sampleValidPipelineYAML()), 0000); err != nil {
		t.Fatalf("writing unreadable file: %v", err)
	}

	// Probe whether the test process can actually read the mode 0000 file (e.g. running as root / UID 0)
	if _, err := os.ReadFile(configPath); err == nil {
		t.Skip("skipping unreadable file test: test environment allows reading mode 0000 files (running as root or filesystem ignores mode)")
	}

	_, err := w.LoadRepositoryConfig(triggerSource)
	if err == nil {
		t.Fatal("expected error on unreadable .drev.yml, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("unreadable .drev.yml must not be reported as ErrConfigNotFound, got: %v", err)
	}
}

func TestResolveConfig_RepositoryConfigAuthoritative(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(sampleValidPipelineYAML()), 0644); err != nil {
		t.Fatalf("writing .drev.yml: %v", err)
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}

	// Repository config must take precedence over fallback
	if p.Name != "repo-pipeline" {
		t.Errorf("expected pipeline name 'repo-pipeline', got %q", p.Name)
	}
	if p.Jobs[0].Name != "test-job" {
		t.Errorf("expected job 'test-job', got %q", p.Jobs[0].Name)
	}
	// No warning must be emitted when repository config is loaded
	if logBuf.Len() != 0 {
		t.Errorf("expected empty log output, got: %q", logBuf.String())
	}
}

func TestResolveConfig_MissingConfig_UsesFallbackWithWarning(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if err != nil {
		t.Fatalf("ResolveConfig failed on fallback: %v", err)
	}

	if p.Name != "fallback-pipeline" {
		t.Errorf("expected fallback pipeline name, got %q", p.Name)
	}
	if p.Jobs[0].Name != "fallback-job" {
		t.Errorf("expected fallback job 'fallback-job', got %q", p.Jobs[0].Name)
	}
	if p.Source != triggerSource {
		t.Errorf("expected Source to match triggerSource, got %+v", p.Source)
	}

	expectedWarning := "[drev] warning: no .drev.yml found in repository root; falling back to host configuration (deprecated)\n"
	if logBuf.String() != expectedWarning {
		t.Errorf("expected warning %q, got %q", expectedWarning, logBuf.String())
	}
}

func TestResolveConfig_FallbackMetadataPreservation_NoPointerMutation(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	originalFallback := sampleHostFallback()
	originalFallback.Source = drevtypes.Source{
		URL:       "https://original.host.example/repo.git",
		CommitSHA: "",
	}

	p, err := w.ResolveConfig(triggerSource, originalFallback, io.Discard)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}

	// Returned pipeline must have triggerSource metadata
	if p.Source.CommitSHA != triggerSource.CommitSHA {
		t.Errorf("expected resolved commit SHA %s, got %s", triggerSource.CommitSHA, p.Source.CommitSHA)
	}

	// Original fallback pointer must NOT be mutated
	if originalFallback.Source.CommitSHA != "" {
		t.Errorf("original fallback was mutated! expected empty CommitSHA, got %s", originalFallback.Source.CommitSHA)
	}
	if p == originalFallback {
		t.Error("expected ResolveConfig to return a shallow copy, but returned identical pointer")
	}
}

func TestResolveConfig_MissingConfig_WithoutFallback_Fails(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	p, err := w.ResolveConfig(triggerSource, nil, io.Discard)
	if p != nil {
		t.Errorf("expected nil pipeline, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error when no config and no fallback, got nil")
	}
	if !errors.Is(err, ErrConfigNotFound) {
		t.Errorf("expected error to wrap ErrConfigNotFound, got: %v", err)
	}
}

func TestResolveConfig_MissingConfig_ZeroJobBootstrap_Rejected(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()

	// Bootstrap pipeline with zero jobs is NOT a valid host fallback
	bootstrapFallback := &drevtypes.Pipeline{
		Name:   "bootstrap-pipeline",
		Source: triggerSource,
		Jobs:   nil,
	}

	p, err := w.ResolveConfig(triggerSource, bootstrapFallback, io.Discard)
	if p != nil {
		t.Errorf("expected nil pipeline, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error when fallback has 0 jobs, got nil")
	}
	if !errors.Is(err, ErrConfigNotFound) {
		t.Errorf("expected error to wrap ErrConfigNotFound, got: %v", err)
	}
}

func TestResolveConfig_MalformedYAML_BypassesFallback(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	malformedYAML := `
name: broken
jobs: [unclosed bracket
`
	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(malformedYAML), 0644); err != nil {
		t.Fatalf("writing .drev.yml: %v", err)
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if p != nil {
		t.Errorf("expected nil pipeline on malformed YAML, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error on malformed YAML, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("malformed YAML must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged on malformed YAML, got: %q", logBuf.String())
	}
}

func TestResolveConfig_InvalidSemantics_BypassesFallback(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	// Valid YAML syntax, but fails parser.Validate (missing job image)
	invalidSemanticYAML := `
name: invalid-semantics
jobs:
  - name: test-job
    steps:
      - name: s1
        run: echo ok
`
	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(invalidSemanticYAML), 0644); err != nil {
		t.Fatalf("writing .drev.yml: %v", err)
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if p != nil {
		t.Errorf("expected nil pipeline on semantically invalid config, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error on semantically invalid config, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("semantically invalid config must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged on semantic failure, got: %q", logBuf.String())
	}
}

func TestResolveConfig_SymlinkEscape_BypassesFallback(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	externalDir := t.TempDir()
	externalConfig := filepath.Join(externalDir, "escape.drev.yml")
	if err := os.WriteFile(externalConfig, []byte(sampleValidPipelineYAML()), 0644); err != nil {
		t.Fatalf("writing external config: %v", err)
	}

	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink(externalConfig, symlinkPath); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if p != nil {
		t.Errorf("expected nil pipeline on symlink escape, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error on symlink escape, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("symlink escape must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if !strings.Contains(err.Error(), "escapes workspace root") {
		t.Errorf("expected error message to contain 'escapes workspace root', got: %v", err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged on symlink escape, got: %q", logBuf.String())
	}
}

func TestResolveConfig_DanglingSymlink_BypassesFallback(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	symlinkPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.Symlink("dangling-file.yml", symlinkPath); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if p != nil {
		t.Errorf("expected nil pipeline on dangling symlink, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error on dangling symlink, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("dangling symlink must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged on dangling symlink, got: %q", logBuf.String())
	}
}

func TestResolveConfig_UnreadableFile_BypassesFallback(t *testing.T) {
	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	configPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := os.WriteFile(configPath, []byte(sampleValidPipelineYAML()), 0000); err != nil {
		t.Fatalf("writing unreadable file: %v", err)
	}

	if _, err := os.ReadFile(configPath); err == nil {
		t.Skip("skipping unreadable file test: test environment allows reading mode 0000 files (running as root or filesystem ignores mode)")
	}

	var logBuf bytes.Buffer
	p, err := w.ResolveConfig(triggerSource, fallback, &logBuf)
	if p != nil {
		t.Errorf("expected nil pipeline on unreadable config, got %+v", p)
	}
	if err == nil {
		t.Fatal("expected error on unreadable config, got nil")
	}
	if errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("unreadable file must not be reported as ErrConfigNotFound, got: %v", err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged on unreadable config, got: %q", logBuf.String())
	}
}

func TestLoadRepositoryConfig_FIFO_RejectedWithoutHanging(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping FIFO test: Windows does not support Unix named pipes / mkfifo")
	}

	w := setupTestWorkspace(t)
	triggerSource := sampleTriggerSource()
	fallback := sampleHostFallback()

	fifoPath := filepath.Join(w.SourceDir(), ".drev.yml")
	if err := syscall.Mkfifo(fifoPath, 0666); err != nil {
		t.Skipf("skipping FIFO test: syscall.Mkfifo failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(fifoPath)
	})

	// 1. Verify LoadRepositoryConfig rejects FIFO by file type without hanging on read
	doneLoad := make(chan struct{})
	var loadErr error
	go func() {
		defer close(doneLoad)
		_, loadErr = w.LoadRepositoryConfig(triggerSource)
	}()

	select {
	case <-doneLoad:
	case <-time.After(2 * time.Second):
		t.Fatal("LoadRepositoryConfig blocked/hung on FIFO instead of rejecting by file type")
	}

	if loadErr == nil {
		t.Fatal("expected error on FIFO, got nil")
	}
	if errors.Is(loadErr, ErrConfigNotFound) {
		t.Fatalf("FIFO must not be reported as ErrConfigNotFound, got: %v", loadErr)
	}
	if !strings.Contains(loadErr.Error(), "not a regular file") {
		t.Errorf("expected error message to contain 'not a regular file', got: %v", loadErr)
	}

	// 2. Verify ResolveConfig with valid fallback does NOT trigger fallback and does not hang
	var logBuf bytes.Buffer
	doneResolve := make(chan struct{})
	var p *drevtypes.Pipeline
	var resolveErr error
	go func() {
		defer close(doneResolve)
		p, resolveErr = w.ResolveConfig(triggerSource, fallback, &logBuf)
	}()

	select {
	case <-doneResolve:
	case <-time.After(2 * time.Second):
		t.Fatal("ResolveConfig blocked/hung on FIFO instead of rejecting by file type")
	}

	if p != nil {
		t.Errorf("expected nil pipeline on FIFO, got %+v", p)
	}
	if resolveErr == nil {
		t.Fatal("expected error on FIFO in ResolveConfig, got nil")
	}
	if errors.Is(resolveErr, ErrConfigNotFound) {
		t.Fatalf("FIFO must not be reported as ErrConfigNotFound, got: %v", resolveErr)
	}
	if logBuf.Len() != 0 {
		t.Errorf("expected no warning logged for FIFO rejection, got: %q", logBuf.String())
	}
}
