package runner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

type mockStore struct {
	store.Store
}

// mockDockerClient implements DockerClient for testing without a live daemon.
type mockDockerClient struct {
	inspectFunc       func(ctx context.Context, image string) (types.ImageInspect, []byte, error)
	pullFunc          func(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error)
	containerCreateFn func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	containerRemoveFn func(ctx context.Context, containerID string, options container.RemoveOptions) error
	containerStartFn  func(ctx context.Context, containerID string, options container.StartOptions) error
	containerLogsFn   func(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error)
	containerWaitFn   func(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error)
}

func (m *mockDockerClient) ImageInspectWithRaw(ctx context.Context, image string) (types.ImageInspect, []byte, error) {
	if m.inspectFunc != nil {
		return m.inspectFunc(ctx, image)
	}
	return types.ImageInspect{}, nil, nil
}

func (m *mockDockerClient) ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
	if m.pullFunc != nil {
		return m.pullFunc(ctx, ref, options)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockDockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
	if m.containerCreateFn != nil {
		return m.containerCreateFn(ctx, config, hostConfig, networkingConfig, platform, containerName)
	}
	return container.CreateResponse{ID: "mock-container-123"}, nil
}

func (m *mockDockerClient) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	if m.containerRemoveFn != nil {
		return m.containerRemoveFn(ctx, containerID, options)
	}
	return nil
}

func (m *mockDockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	if m.containerStartFn != nil {
		return m.containerStartFn(ctx, containerID, options)
	}
	return nil
}

func (m *mockDockerClient) ContainerLogs(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error) {
	if m.containerLogsFn != nil {
		return m.containerLogsFn(ctx, container, options)
	}
	// Return valid multiplexed stdout stream using stdcopy format
	var buf bytes.Buffer
	w := stdcopy.NewStdWriter(&buf, stdcopy.Stdout)
	w.Write([]byte("mock container output\n"))
	return io.NopCloser(&buf), nil
}

func (m *mockDockerClient) ContainerWait(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error) {
	if m.containerWaitFn != nil {
		return m.containerWaitFn(ctx, containerID, condition)
	}
	statusCh := make(chan container.WaitResponse, 1)
	errCh := make(chan error, 1)
	statusCh <- container.WaitResponse{StatusCode: 0}
	return statusCh, errCh
}

// --- Unit Tests (No Live Docker Daemon Required) ---

func TestRunJob_ImageInspect_GenuineNotFound_TriggersPull(t *testing.T) {
	pullCalled := false
	mock := &mockDockerClient{
		inspectFunc: func(ctx context.Context, image string) (types.ImageInspect, []byte, error) {
			return types.ImageInspect{}, nil, errdefs.NotFound(errors.New("No such image: my-test-image"))
		},
		pullFunc: func(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
			pullCalled = true
			jsonStream := `{"status":"Pulling from library/my-test-image"}` + "\n" + `{"status":"Digest: sha256:123"}` + "\n"
			return io.NopCloser(strings.NewReader(jsonStream)), nil
		},
	}

	r := NewWithClient(mock, &mockStore{})
	job := &drevtypes.Job{
		Name:  "test-not-found",
		Image: "my-test-image",
		Steps: []drevtypes.Step{{Name: "s1", Run: "echo hi"}},
	}

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err := r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err != nil {
		t.Fatalf("RunJob unexpected error: %v", err)
	}

	if !pullCalled {
		t.Errorf("expected ImagePull to be called for genuine not-found error, but was not")
	}

	out := buf.String()
	if !strings.Contains(out, "[drev] image not found locally, pulling: my-test-image") {
		t.Errorf("output missing pulling status message: %s", out)
	}
}

func TestRunJob_ImageInspect_NonNotFoundError_DoesNotPull(t *testing.T) {
	pullCalled := false
	mock := &mockDockerClient{
		inspectFunc: func(ctx context.Context, image string) (types.ImageInspect, []byte, error) {
			return types.ImageInspect{}, nil, errors.New("cannot connect to the Docker daemon at unix:///var/run/docker.sock")
		},
		pullFunc: func(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
			pullCalled = true
			return io.NopCloser(strings.NewReader("")), nil
		},
	}

	r := NewWithClient(mock, &mockStore{})
	job := &drevtypes.Job{
		Name:  "test-daemon-fail",
		Image: "my-test-image",
		Steps: []drevtypes.Step{{Name: "s1", Run: "echo hi"}},
	}

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err := r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err == nil {
		t.Fatal("expected error from RunJob when inspection fails with non-not-found error, got nil")
	}

	if pullCalled {
		t.Errorf("ImagePull should NOT be called on non-not-found inspect errors")
	}

	if !strings.Contains(err.Error(), "inspecting image") || !strings.Contains(err.Error(), "cannot connect to the Docker daemon") {
		t.Errorf("expected wrapped inspect error, got: %v", err)
	}
}

func TestRunJob_ImageInspect_Cached_DoesNotPull(t *testing.T) {
	pullCalled := false
	mock := &mockDockerClient{
		inspectFunc: func(ctx context.Context, image string) (types.ImageInspect, []byte, error) {
			return types.ImageInspect{ID: "sha256:localcached"}, nil, nil
		},
		pullFunc: func(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
			pullCalled = true
			return io.NopCloser(strings.NewReader("")), nil
		},
	}

	r := NewWithClient(mock, &mockStore{})
	job := &drevtypes.Job{
		Name:  "test-cached",
		Image: "cached-image:v1",
		Steps: []drevtypes.Step{{Name: "s1", Run: "echo hi"}},
	}

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err := r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err != nil {
		t.Fatalf("RunJob unexpected error: %v", err)
	}

	if pullCalled {
		t.Errorf("ImagePull should NOT be called when image is cached")
	}

	out := buf.String()
	if !strings.Contains(out, "[drev] using cached image: cached-image:v1") {
		t.Errorf("output missing cached status message: %s", out)
	}
}

func TestProcessPullStream_Success(t *testing.T) {
	stream := `{"status":"Pulling fs layer","id":"e4c0d831c59c"}` + "\n" +
		`{"status":"Downloading","progressDetail":{"current":100,"total":100},"id":"e4c0d831c59c"}` + "\n" +
		`{"status":"Download complete","id":"e4c0d831c59c"}` + "\n" +
		`{"status":"Digest: sha256:abc123"}` + "\n" +
		`{"status":"Status: Downloaded newer image for alpine:latest"}` + "\n"

	var buf bytes.Buffer
	err := processPullStream(strings.NewReader(stream), &buf)
	if err != nil {
		t.Fatalf("processPullStream failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "e4c0d831c59c: Pulling fs layer") {
		t.Errorf("expected formatted ID/status line, got: %s", out)
	}
	if !strings.Contains(out, "Status: Downloaded newer image for alpine:latest") {
		t.Errorf("expected final status line, got: %s", out)
	}
}

func TestProcessPullStream_ErrorMessage(t *testing.T) {
	// Deprecated ErrorMessage field
	stream := `{"status":"Pulling from library/private"}` + "\n" +
		`{"error":"pull access denied, repository does not exist or may require authorization"}` + "\n"

	var buf bytes.Buffer
	err := processPullStream(strings.NewReader(stream), &buf)
	if err == nil {
		t.Fatal("expected error from processPullStream, got nil")
	}

	if !strings.Contains(err.Error(), "pull access denied") {
		t.Errorf("expected error to contain pull error message, got: %v", err)
	}
}

func TestProcessPullStream_ErrorObject(t *testing.T) {
	// Structured ErrorDetail (Error.Message and Error.Code)
	stream := `{"status":"Pulling from library/nonexistent"}` + "\n" +
		`{"errorDetail":{"message":"manifest for library/nonexistent:latest not found: manifest unknown","code":404},"error":"manifest for library/nonexistent:latest not found: manifest unknown"}` + "\n"

	var buf bytes.Buffer
	err := processPullStream(strings.NewReader(stream), &buf)
	if err == nil {
		t.Fatal("expected error from processPullStream, got nil")
	}

	if !strings.Contains(err.Error(), "manifest unknown") {
		t.Errorf("expected error to contain Error.Message, got: %v", err)
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error to contain Error.Code, got: %v", err)
	}
}

func TestProcessPullStream_MalformedJSON(t *testing.T) {
	stream := `{"status":"Pulling from library/alpine"` + "\n" + `broken json`

	var buf bytes.Buffer
	err := processPullStream(strings.NewReader(stream), &buf)
	if err == nil {
		t.Fatal("expected decode error for malformed stream, got nil")
	}
	if !strings.Contains(err.Error(), "decoding pull stream") {
		t.Errorf("expected decoding pull stream error, got: %v", err)
	}
}

type errReader struct {
	err error
}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func TestProcessPullStream_ReadError(t *testing.T) {
	reader := &errReader{err: errors.New("abrupt network reset")}
	var buf bytes.Buffer
	err := processPullStream(reader, &buf)
	if err == nil {
		t.Fatal("expected read error to propagate, got nil")
	}
	if !strings.Contains(err.Error(), "abrupt network reset") {
		t.Errorf("expected error to contain underlying read error, got: %v", err)
	}
}

type errWriter struct {
	err error
}

func (e *errWriter) Write(p []byte) (n int, err error) {
	return 0, e.err
}

func TestProcessPullStream_WriterError(t *testing.T) {
	stream := `{"status":"Pulling from library/alpine"}` + "\n"
	writer := &errWriter{err: errors.New("disk write failure")}
	err := processPullStream(strings.NewReader(stream), writer)
	if err == nil {
		t.Fatal("expected writer error to propagate, got nil")
	}
	if !strings.Contains(err.Error(), "disk write failure") {
		t.Errorf("expected error to contain writer error, got: %v", err)
	}
}

// --- Integration Tests (Skipped if Docker daemon unreachable) ---

func TestRunJob_Success(t *testing.T) {
	cli, err := NewDockerClient()
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

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err != nil {
		t.Errorf("RunJob() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "hello drev") {
		t.Errorf("RunJob() output = %q, want to contain 'hello drev'", out)
	}
}

func TestRunJob_Failure(t *testing.T) {
	cli, err := NewDockerClient()
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

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err == nil {
		t.Fatalf("RunJob() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "exit code 1") {
		t.Errorf("RunJob() error = %v, want to contain 'exit code 1'", err)
	}
}

func TestRunJob_MultiStep(t *testing.T) {
	cli, err := NewDockerClient()
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

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
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
	cli, err := NewDockerClient()
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

	w, _ := workspace.Create()
	defer w.Cleanup()

	var buf bytes.Buffer
	err = r.RunJob(context.Background(), &drevtypes.Run{}, job, w, &buf)
	if err != nil {
		t.Errorf("RunJob() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "hello123") {
		t.Errorf("RunJob() output = %q, want to contain 'hello123'", out)
	}
}
