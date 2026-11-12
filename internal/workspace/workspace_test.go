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

func createMultiCommitRepo(t *testing.T) (repoDir string, commit1 string, commit2 string) {
	t.Helper()

	repo := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(filepath.Join(repo, "hello"), 0755); err != nil {
		t.Fatal(err)
	}

	gitCmd := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}

	gitCmd("init", "-b", "main")
	gitCmd("config", "user.email", "test@example.com")
	gitCmd("config", "user.name", "Drev Test")

	file := filepath.Join(repo, "hello", "hello.go")
	if err := os.WriteFile(file, []byte("package hello\n// version 1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	gitCmd("add", ".")
	gitCmd("commit", "-m", "commit 1")
	commit1 = gitCmd("rev-parse", "HEAD")

	if err := os.WriteFile(file, []byte("package hello\n// version 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	gitCmd("add", ".")
	gitCmd("commit", "-m", "commit 2")
	commit2 = gitCmd("rev-parse", "HEAD")

	return repo, commit1, commit2
}

func getRepoHeadSHA(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "rev-parse", "HEAD")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rev-parse HEAD in %s: %v\n%s", dir, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestClone_ExactCommitSHA(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, commit1, _ := createMultiCommitRepo(t)

	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	src := drevtypes.Source{
		Type: "git",
		URL:  repo,
		Ref:  commit1,
	}

	var buf bytes.Buffer
	if err := w.Clone(context.Background(), src, &buf); err != nil {
		t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
	}

	head := getRepoHeadSHA(t, w.SourceDir())
	if !strings.EqualFold(head, commit1) {
		t.Errorf("expected HEAD %s, got %s", commit1, head)
	}

	content, err := os.ReadFile(filepath.Join(w.SourceDir(), "hello", "hello.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "// version 1") {
		t.Errorf("expected version 1 content, got: %s", string(content))
	}
}

func TestClone_BranchDrift(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	// Repo main branch is advanced to commit2, but we request the historical commit1
	repo, commit1, commit2 := createMultiCommitRepo(t)

	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	src := drevtypes.Source{
		Type: "git",
		URL:  repo,
		Ref:  commit1,
	}

	var buf bytes.Buffer
	if err := w.Clone(context.Background(), src, &buf); err != nil {
		t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
	}

	head := getRepoHeadSHA(t, w.SourceDir())
	if !strings.EqualFold(head, commit1) {
		t.Errorf("expected HEAD to remain at target commit %s, got %s (branch tip was %s)", commit1, head, commit2)
	}

	content, err := os.ReadFile(filepath.Join(w.SourceDir(), "hello", "hello.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "// version 1") {
		t.Errorf("expected version 1 content, got: %s", string(content))
	}
	if strings.Contains(string(content), "// version 2") {
		t.Errorf("unexpectedly checked out latest branch tip version 2 instead of target commit 1")
	}
}

func TestClone_BranchCompatibility(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, _, commit2 := createMultiCommitRepo(t)

	t.Run("explicit branch name", func(t *testing.T) {
		w, err := Create()
		if err != nil {
			t.Fatalf("Create() error: %v", err)
		}
		defer w.Cleanup()

		src := drevtypes.Source{
			Type: "git",
			URL:  repo,
			Ref:  "main",
		}

		var buf bytes.Buffer
		if err := w.Clone(context.Background(), src, &buf); err != nil {
			t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
		}

		head := getRepoHeadSHA(t, w.SourceDir())
		if !strings.EqualFold(head, commit2) {
			t.Errorf("expected branch tip HEAD %s, got %s", commit2, head)
		}
	})

	t.Run("empty ref defaults to main", func(t *testing.T) {
		w, err := Create()
		if err != nil {
			t.Fatalf("Create() error: %v", err)
		}
		defer w.Cleanup()

		src := drevtypes.Source{
			Type: "git",
			URL:  repo,
			Ref:  "",
		}

		var buf bytes.Buffer
		if err := w.Clone(context.Background(), src, &buf); err != nil {
			t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
		}

		head := getRepoHeadSHA(t, w.SourceDir())
		if !strings.EqualFold(head, commit2) {
			t.Errorf("expected default branch tip HEAD %s, got %s", commit2, head)
		}
	})
}

func TestClone_MissingOrInvalidSHA(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, _, _ := createMultiCommitRepo(t)

	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	src := drevtypes.Source{
		Type: "git",
		URL:  repo,
		Ref:  "0000000000000000000000000000000000000000",
	}

	var buf bytes.Buffer
	err = w.Clone(context.Background(), src, &buf)
	if err == nil {
		t.Fatal("expected error for non-existent commit SHA, got nil")
	}

	if !strings.Contains(err.Error(), "git clone failed") {
		t.Errorf("expected error to contain 'git clone failed', got: %v", err)
	}

	// Verify cleanup: sourceDir must be removed on failure
	if _, statErr := os.Stat(w.SourceDir()); !os.IsNotExist(statErr) {
		t.Errorf("expected sourceDir to be cleaned up after failure, but it exists: %s", w.SourceDir())
	}
}

func TestClone_FailedCleanup(t *testing.T) {
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
		URL:  filepath.Join(t.TempDir(), "nonexistent-dir"),
		Ref:  "main",
	}

	var buf bytes.Buffer
	err = w.Clone(context.Background(), src, &buf)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if _, statErr := os.Stat(w.SourceDir()); !os.IsNotExist(statErr) {
		t.Errorf("expected sourceDir to be removed on failure, got err: %v", statErr)
	}
}

func TestClone_HEADVerification_Mismatch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, commit1, commit2 := createMultiCommitRepo(t)

	// In the remote repo, set up a ref pointing to commit1
	cmd := exec.Command("git", "-C", repo, "update-ref", "refs/pull/42/head", commit1)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("update-ref: %v\n%s", err, out)
	}

	w, err := Create()
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	defer w.Cleanup()

	// Request refs/pull/42/head, but assert that it must match commit2 (which it does NOT)
	src := drevtypes.Source{
		Type: "git",
		URL:  repo,
		Ref:  "refs/pull/42/head@" + commit2,
	}

	var buf bytes.Buffer
	err = w.Clone(context.Background(), src, &buf)
	if err == nil {
		t.Fatal("expected error on commit mismatch, got nil")
	}

	if !strings.Contains(err.Error(), "commit mismatch") {
		t.Errorf("expected error to contain 'commit mismatch', got: %v", err)
	}

	if _, statErr := os.Stat(w.SourceDir()); !os.IsNotExist(statErr) {
		t.Errorf("expected sourceDir to be removed on verification failure")
	}
}

func TestClone_PRRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, commit1, _ := createMultiCommitRepo(t)

	// Create PR ref in remote
	cmd := exec.Command("git", "-C", repo, "update-ref", "refs/pull/99/head", commit1)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("update-ref: %v\n%s", err, out)
	}

	t.Run("PR ref without explicit SHA", func(t *testing.T) {
		w, err := Create()
		if err != nil {
			t.Fatalf("Create() error: %v", err)
		}
		defer w.Cleanup()

		src := drevtypes.Source{
			Type: "git",
			URL:  repo,
			Ref:  "refs/pull/99/head",
		}

		var buf bytes.Buffer
		if err := w.Clone(context.Background(), src, &buf); err != nil {
			t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
		}

		head := getRepoHeadSHA(t, w.SourceDir())
		if !strings.EqualFold(head, commit1) {
			t.Errorf("expected PR HEAD %s, got %s", commit1, head)
		}
	})

	t.Run("PR ref with matching explicit SHA", func(t *testing.T) {
		w, err := Create()
		if err != nil {
			t.Fatalf("Create() error: %v", err)
		}
		defer w.Cleanup()

		src := drevtypes.Source{
			Type: "git",
			URL:  repo,
			Ref:  "refs/pull/99/head@" + commit1,
		}

		var buf bytes.Buffer
		if err := w.Clone(context.Background(), src, &buf); err != nil {
			t.Fatalf("Clone() error: %v\noutput: %s", err, buf.String())
		}

		head := getRepoHeadSHA(t, w.SourceDir())
		if !strings.EqualFold(head, commit1) {
			t.Errorf("expected PR HEAD %s, got %s", commit1, head)
		}
	})
}

func TestClone_OptionInjectionRejected(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	repo, commit1, _ := createMultiCommitRepo(t)

	tests := []struct {
		name string
		ref  string
	}{
		{
			name: "double dash flag",
			ref:  "--upload-pack=touch /tmp/evil",
		},
		{
			name: "single dash flag",
			ref:  "-u",
		},
		{
			name: "depth option flag",
			ref:  "--depth=1",
		},
		{
			name: "flag in prefix before @sha",
			ref:  "--upload-pack@" + commit1,
		},
		{
			name: "flag in target after @",
			ref:  "main@--upload-pack",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, err := Create()
			if err != nil {
				t.Fatalf("Create() error: %v", err)
			}
			defer w.Cleanup()

			src := drevtypes.Source{
				Type: "git",
				URL:  repo,
				Ref:  tc.ref,
			}

			var buf bytes.Buffer
			err = w.Clone(context.Background(), src, &buf)
			if err == nil {
				t.Fatalf("expected error for option-like ref %q, got nil", tc.ref)
			}

			if !strings.Contains(err.Error(), "ref name cannot start with '-'") {
				t.Errorf("expected error to mention 'ref name cannot start with \"-\"', got: %v", err)
			}
			if !strings.Contains(err.Error(), "git clone failed") {
				t.Errorf("expected error to contain 'git clone failed', got: %v", err)
			}

			// Verify sourceDir is cleaned up on validation failure
			if _, statErr := os.Stat(w.SourceDir()); !os.IsNotExist(statErr) {
				t.Errorf("expected sourceDir to be cleaned up after validation failure: %s", w.SourceDir())
			}
		})
	}
}

func TestClone_CredentialMasking(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	testCases := []struct {
		name        string
		url         string
		secretParts []string
	}{
		{
			name:        "standard token",
			url:         "https://ghp_superSecretToken12345XYZ:x-oauth-basic@invalid.internal.domain/org/repo.git",
			secretParts: []string{"ghp_superSecretToken12345XYZ", "x-oauth-basic"},
		},
		{
			name:        "unencoded at in userinfo",
			url:         "https://user@sub.domain.com:mySecretPassword999@invalid.internal.domain/org/repo.git",
			secretParts: []string{"user@sub.domain.com", "mySecretPassword999"},
		},
		{
			name:        "colons in password",
			url:         "https://admin:p:a:s:s:w:o:r:d:777@invalid.internal.domain/org/repo.git",
			secretParts: []string{"p:a:s:s:w:o:r:d:777"},
		},
		{
			name:        "percent-encoded characters in password",
			url:         "https://user:p%40ss%20word%21@invalid.internal.domain/org/repo.git",
			secretParts: []string{"p%40ss%20word%21"},
		},
		{
			name:        "query and fragment characters in password",
			url:         "https://token:secret?key=value#frag@invalid.internal.domain/org/repo.git",
			secretParts: []string{"secret?key=value#frag"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w, err := Create()
			if err != nil {
				t.Fatalf("Create() error: %v", err)
			}
			defer w.Cleanup()

			src := drevtypes.Source{
				Type: "git",
				URL:  tc.url,
				Ref:  "main",
			}

			var buf bytes.Buffer
			err = w.Clone(context.Background(), src, &buf)
			if err == nil {
				t.Fatal("expected clone to fail for nonexistent host, got nil")
			}

			// Verify error does NOT contain any secret parts
			for _, secret := range tc.secretParts {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("credential part %q leaked in returned error: %v", secret, err)
				}
			}
			if !strings.Contains(err.Error(), "***@") {
				t.Errorf("expected error to contain masked credentials '***@', got: %v", err)
			}

			// Verify logWriter output does NOT contain any secret parts
			logOutput := buf.String()
			for _, secret := range tc.secretParts {
				if strings.Contains(logOutput, secret) {
					t.Errorf("credential part %q leaked in logWriter output: %s", secret, logOutput)
				}
			}
			if !strings.Contains(logOutput, "***@") {
				t.Errorf("expected log output to contain masked credentials '***@', got: %s", logOutput)
			}
		})
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
