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

	helloPath := filepath.Join(w.Dir, "hello", "hello.go")
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
