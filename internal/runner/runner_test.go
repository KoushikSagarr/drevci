package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/docker/docker/client"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
)

type mockStore struct {
	store.Store
}

func TestRunJob_Success(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("docker not available")
	}
	if _, err := cli.Ping(context.Background()); err != nil {
		t.Skip("docker daemon unreachable")
	}

	r, err := New(&mockStore{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := &drevtypes.Job{
		Name:  "test-success",
		Image: "alpine:latest",
		Steps: []drevtypes.Step{
			{Name: "step1", Run: "echo hello drev"},
		},
	}

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, &buf)
	if err != nil {
		t.Errorf("RunJob() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "hello drev") {
		t.Errorf("RunJob() output = %q, want to contain 'hello drev'", out)
	}
}

func TestRunJob_Failure(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("docker not available")
	}
	if _, err := cli.Ping(context.Background()); err != nil {
		t.Skip("docker daemon unreachable")
	}

	r, err := New(&mockStore{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := &drevtypes.Job{
		Name:  "test-fail",
		Image: "alpine:latest",
		Steps: []drevtypes.Step{
			{Name: "fail", Run: "exit 1"},
		},
	}

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, &buf)
	if err == nil {
		t.Fatalf("RunJob() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "exit code 1") {
		t.Errorf("RunJob() error = %v, want to contain 'exit code 1'", err)
	}
}

func TestRunJob_MultiStep(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("docker not available")
	}
	if _, err := cli.Ping(context.Background()); err != nil {
		t.Skip("docker daemon unreachable")
	}

	r, err := New(&mockStore{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := &drevtypes.Job{
		Name:  "test-multi",
		Image: "alpine:latest",
		Steps: []drevtypes.Step{
			{Name: "s1", Run: "echo step1"},
			{Name: "s2", Run: "echo step2"},
			{Name: "s3", Run: "echo step3"},
		},
	}

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, &buf)
	if err != nil {
		t.Errorf("RunJob() error = %v", err)
	}

	out := buf.String()
	i1 := strings.Index(out, "step1")
	i2 := strings.Index(out, "step2")
	i3 := strings.Index(out, "step3")
	if i1 == -1 || i2 == -1 || i3 == -1 {
		t.Errorf("RunJob() output missing steps: %q", out)
	}
	if !(i1 < i2 && i2 < i3) {
		t.Errorf("RunJob() output steps out of order: %q", out)
	}
}

func TestRunJob_EnvVars(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("docker not available")
	}
	if _, err := cli.Ping(context.Background()); err != nil {
		t.Skip("docker daemon unreachable")
	}

	r, err := New(&mockStore{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := &drevtypes.Job{
		Name:  "test-env",
		Image: "alpine:latest",
		Env: map[string]string{
			"DREV_TEST_VAR": "hello123",
		},
		Steps: []drevtypes.Step{
			{Name: "env", Run: "echo $DREV_TEST_VAR"},
		},
	}

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, &buf)
	if err != nil {
		t.Errorf("RunJob() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "hello123") {
		t.Errorf("RunJob() output = %q, want to contain 'hello123'", out)
	}
}
