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

func TestClone_PublicRepo(t *testing.T) {
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
		URL:  "https://github.com/golang/example.git",
		Ref:  "master",
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
		URL:  "https://github.com/nonexistent/repo-that-does-not-exist.git",
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
