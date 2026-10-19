package workspace

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/drevci/drev/pkg/drevtypes"
)

func TestCreate(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if _, err := os.Stat(w.Dir); os.IsNotExist(err) {
		t.Errorf("expected dir to exist: %s", w.Dir)
	}

	err = w.Cleanup()
	if err != nil {
		t.Errorf("Cleanup() error: %v", err)
	}

	if _, err := os.Stat(w.Dir); !os.IsNotExist(err) {
		t.Errorf("expected dir to be removed: %s", w.Dir)
	}
}

func TestClone_NoSource(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	var buf bytes.Buffer
	err = w.Clone(context.Background(), drevtypes.Source{}, &buf)
	if err != nil {
		t.Errorf("Clone() error: %v", err)
	}
	if buf.Len() > 0 {
		t.Errorf("expected no output, got: %s", buf.String())
	}
}

func createLocalGitRepo(t *testing.T) string {
	t.Helper()

	repo := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(filepath.Join(repo, "hello"), 0755); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}

	run("init", "-b", "main")
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "Drev Test")

	file := filepath.Join(repo, "hello", "hello.go")
	if err := os.WriteFile(file, []byte("package hello\n"), 0644); err != nil {
		t.Fatal(err)
	}

	run("add", ".")
	run("commit", "-m", "initial test commit")

	return repo
}

func TestClone_PublicRepo(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	src := drevtypes.Source{
		Type: "git",
		URL:  createLocalGitRepo(t),
		Ref:  "main",
	}

	var buf bytes.Buffer
	err = w.Clone(context.Background(), src, &buf)
	if err != nil {
		t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
	}

	helloPath := filepath.Join(w.SourceDir(), "hello", "hello.go")
	if _, err := os.Stat(helloPath); os.IsNotExist(err) {
		t.Errorf("expected %s to exist", helloPath)
	}
}

func TestClone_InvalidURL(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	src := drevtypes.Source{
		Type: "git",
		URL:  filepath.Join(t.TempDir(), "nonexistent-repo"),
		Ref:  "main",
	}

	var buf bytes.Buffer
	err = w.Clone(context.Background(), src, &buf)
	if err == nil {
		t.Fatalf("Clone() expected error, got nil")
	}

	if !strings.Contains(err.Error(), "git clone failed") {
		t.Errorf("expected error to contain 'git clone failed', got: %v", err)
	}
}

func TestCreateJobWorkspace_RootJob(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	// Populate central source directory
	if err := os.MkdirAll(w.SourceDir(), 0755); err != nil {
		t.Fatal(err)
	}
	sourceFile := filepath.Join(w.SourceDir(), "app.go")
	if err := os.WriteFile(sourceFile, []byte("package main\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create root job workspace
	jobW, err := w.CreateJobWorkspace("test-job", "")
	if err != nil {
		t.Fatalf("CreateJobWorkspace() error = %v", err)
	}

	expectedDir := w.JobDir("test-job")
	if jobW.Dir != expectedDir {
		t.Errorf("expected Dir %s, got %s", expectedDir, jobW.Dir)
	}

	copiedFile := filepath.Join(jobW.Dir, "app.go")
	content, err := os.ReadFile(copiedFile)
	if err != nil {
		t.Fatalf("reading copied file: %v", err)
	}
	if string(content) != "package main\n" {
		t.Errorf("expected 'package main\\n', got %q", string(content))
	}

	// Modify job workspace and assert central source is untouched (immutable)
	if err := os.WriteFile(copiedFile, []byte("package modified\n"), 0644); err != nil {
		t.Fatal(err)
	}
	origContent, err := os.ReadFile(sourceFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(origContent) != "package main\n" {
		t.Errorf("central source was modified! got %q", string(origContent))
	}
}

func TestCreateJobWorkspace_SingleParentInheritance(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	// Create parent job workspace and write an artifact
	parentW, err := w.CreateJobWorkspace("build", "")
	if err != nil {
		t.Fatalf("creating parent workspace: %v", err)
	}
	artifactPath := filepath.Join(parentW.Dir, "drev-binary")
	if err := os.WriteFile(artifactPath, []byte("compiled binary data"), 0755); err != nil {
		t.Fatal(err)
	}

	// Create child job inheriting from "build"
	childW, err := w.CreateJobWorkspace("deploy", "build")
	if err != nil {
		t.Fatalf("creating child workspace: %v", err)
	}

	childArtifact := filepath.Join(childW.Dir, "drev-binary")
	content, err := os.ReadFile(childArtifact)
	if err != nil {
		t.Fatalf("reading inherited artifact: %v", err)
	}
	if string(content) != "compiled binary data" {
		t.Errorf("expected 'compiled binary data', got %q", string(content))
	}

	// Modify child artifact and verify parent is not affected
	if err := os.WriteFile(childArtifact, []byte("child modified binary"), 0755); err != nil {
		t.Fatal(err)
	}
	parentContent, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(parentContent) != "compiled binary data" {
		t.Errorf("parent artifact was unexpectedly modified: got %q", string(parentContent))
	}
}

func TestCreateJobWorkspace_InvalidParent(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	_, err = w.CreateJobWorkspace("child", "nonexistent-parent")
	if err == nil {
		t.Fatal("expected error when parent workspace does not exist, got nil")
	}
	if !strings.Contains(err.Error(), "parent workspace \"nonexistent-parent\" is not available") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCreateJobWorkspace_PathTraversalGuards(t *testing.T) {
	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	invalidNames := []string{
		"../escape",
		"foo/bar",
		"foo\\bar",
		"",
		".",
	}

	for _, name := range invalidNames {
		_, err := w.CreateJobWorkspace(name, "")
		if err == nil {
			t.Errorf("expected error for invalid job name %q, got nil", name)
		}
	}
}

func TestCopyDir_SymlinksAndPermissions(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	dstDir := filepath.Join(t.TempDir(), "dst")

	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}

	targetFile := filepath.Join(srcDir, "real.txt")
	if err := os.WriteFile(targetFile, []byte("real content"), 0644); err != nil {
		t.Fatal(err)
	}

	symlinkPath := filepath.Join(srcDir, "link.txt")
	if err := os.Symlink("real.txt", symlinkPath); err != nil {
		t.Fatal(err)
	}

	if err := CopyDir(srcDir, dstDir); err != nil {
		t.Fatalf("CopyDir failed: %v", err)
	}

	dstSymlink := filepath.Join(dstDir, "link.txt")
	info, err := os.Lstat(dstSymlink)
	if err != nil {
		t.Fatalf("stat on copied symlink: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("expected symlink at %s, got mode %v", dstSymlink, info.Mode())
	}

	linkTarget, err := os.Readlink(dstSymlink)
	if err != nil {
		t.Fatalf("reading link target: %v", err)
	}
	if linkTarget != "real.txt" {
		t.Errorf("expected link target 'real.txt', got %q", linkTarget)
	}
}
