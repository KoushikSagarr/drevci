package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// DockerClient defines the subset of Docker client operations used by Runner.
type DockerClient interface {
	ImageInspectWithRaw(ctx context.Context, image string) (types.ImageInspect, []byte, error)
	ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerList(ctx context.Context, options container.ListOptions) ([]types.Container, error)
	ContainerLogs(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error)
	ContainerWait(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error)
}

// Runner handles the execution of jobs in Docker containers.
type Runner struct {
	docker DockerClient
	store  store.Store
	limits ResourceLimits
}

type JobResult struct {
	JobName  string
	ExitCode int
	Error    error
	Duration time.Duration
}

// NewDockerClient creates a standard Docker API client with environment and OS-specific socket fallback.
func NewDockerClient() (*client.Client, error) {
	opts := []client.Opt{
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	}

	if os.Getenv("DOCKER_HOST") == "" {
		if _, err := os.Stat("/var/run/docker.sock"); err != nil && os.IsNotExist(err) {
			if home, err := os.UserHomeDir(); err == nil {
				userSocket := filepath.Join(home, ".docker", "run", "docker.sock")
				if _, err := os.Stat(userSocket); err == nil {
					opts = append(opts, client.WithHost("unix://"+userSocket))
				}
			}
		}
	}

	return client.NewClientWithOpts(opts...)
}

// New creates a Runner using standard Docker client environment configuration and validated resource limits.
func New(store store.Store) (*Runner, error) {
	cli, err := NewDockerClient()
	if err != nil {
		return nil, fmt.Errorf("creating docker client: %w", err)
	}
	limits, err := LoadResourceLimitsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("loading resource limits: %w", err)
	}
	return NewWithLimits(cli, store, limits), nil
}

// NewWithClient creates a Runner with a provided DockerClient implementation and environment/default limits.
func NewWithClient(docker DockerClient, store store.Store) *Runner {
	limits, err := LoadResourceLimitsFromEnv()
	if err != nil {
		limits = DefaultResourceLimits()
	}
	return NewWithLimits(docker, store, limits)
}

// NewWithLimits creates a Runner with an explicit DockerClient, store, and resource limits.
func NewWithLimits(docker DockerClient, store store.Store, limits ResourceLimits) *Runner {
	return &Runner{
		docker: docker,
		store:  store,
		limits: limits,
	}
}

// Limits returns the active ResourceLimits enforced by this runner.
func (r *Runner) Limits() ResourceLimits {
	return r.limits
}

// processPullStream reads Docker's JSON message stream from the pull response,
// formats readable progress to logWriter, and returns an error if any pull failure
// or stream decode/read/write error occurs.
func processPullStream(reader io.Reader, logWriter io.Writer) error {
	dec := json.NewDecoder(reader)
	for {
		var jm jsonmessage.JSONMessage
		if err := dec.Decode(&jm); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decoding pull stream: %w", err)
		}

		if jm.Error != nil {
			if jm.Error.Message != "" {
				return fmt.Errorf("docker reported error: %s (code %d)", jm.Error.Message, jm.Error.Code)
			}
			return fmt.Errorf("docker reported error code %d", jm.Error.Code)
		}
		if jm.ErrorMessage != "" {
			return fmt.Errorf("docker reported error: %s", jm.ErrorMessage)
		}

		var line string
		if jm.Stream != "" {
			line = jm.Stream
		} else if jm.ProgressMessage != "" {
			line = fmt.Sprintf("%s %s\n", jm.Status, jm.ProgressMessage)
		} else if jm.Status != "" {
			if jm.ID != "" {
				line = fmt.Sprintf("%s: %s\n", jm.ID, jm.Status)
			} else {
				line = fmt.Sprintf("%s\n", jm.Status)
			}
		}

		if line != "" {
			if _, err := logWriter.Write([]byte(line)); err != nil {
				return fmt.Errorf("writing pull log: %w", err)
			}
		}
	}
	return nil
}

func (r *Runner) RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
	// Check if image exists locally to save time
	_, _, err := r.docker.ImageInspectWithRaw(ctx, job.Image)
	if err == nil {
		fmt.Fprintf(logWriter, "[drev] using cached image: %s\n", job.Image)
	} else if errdefs.IsNotFound(err) {
		fmt.Fprintf(logWriter, "[drev] image not found locally, pulling: %s\n", job.Image)
		reader, pullErr := r.docker.ImagePull(ctx, job.Image, image.PullOptions{})
		if pullErr != nil {
			return fmt.Errorf("pulling image: %w", pullErr)
		}
		defer reader.Close()

		if err := processPullStream(reader, logWriter); err != nil {
			return fmt.Errorf("pulling image %q: %w", job.Image, err)
		}
	} else {
		return fmt.Errorf("inspecting image %q: %w", job.Image, err)
	}

	var cmds []string
	for _, step := range job.Steps {
		cmds = append(cmds, fmt.Sprintf("echo '[drev] --- step: %s ---'", step.Name))
		cmds = append(cmds, step.Run)
	}
	shCmd := strings.Join(cmds, " && ")

	var envVars []string
	for k, v := range job.Env {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}

	pidsLimit := r.limits.PidsLimit

	fmt.Fprintf(logWriter, "[drev] enforcing container limits: %.1f CPUs, %dMB RAM, %s swap, %d PIDs\n",
		float64(r.limits.NanoCPUs)/1e9,
		r.limits.Memory/(1024*1024),
		formatSwapLimit(r.limits),
		r.limits.PidsLimit,
	)

	labels := map[string]string{
		LabelManagedBy: "true",
		LabelRunID:     run.ID,
		LabelJobName:   job.Name,
	}

	resp, err := r.docker.ContainerCreate(ctx, &container.Config{
		Image:      job.Image,
		Cmd:        []string{"sh", "-c", shCmd},
		WorkingDir: "/workspace",
		Env:        envVars,
		Labels:     labels,
	}, &container.HostConfig{
		Binds: []string{w.Dir + ":/workspace"},
		Resources: container.Resources{
			NanoCPUs:   r.limits.NanoCPUs,
			Memory:     r.limits.Memory,
			MemorySwap: r.limits.MemorySwap,
			PidsLimit:  &pidsLimit,
		},
	}, nil, nil, "")
	if err != nil {
		return fmt.Errorf("creating container: %w", err)
	}

	containerID := resp.ID
	defer func() {
		r.docker.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
	}()

	if err := r.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting container: %w", err)
	}

	logsReader, err := r.docker.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return fmt.Errorf("getting container logs: %w", err)
	}
	defer logsReader.Close()

	// Use stdcopy to demultiplex stdout/stderr properly
	_, err = stdcopy.StdCopy(logWriter, logWriter, logsReader)
	if err != nil {
		return fmt.Errorf("streaming logs: %w", err)
	}

	statusCh, errCh := r.docker.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("waiting for container: %w", err)
		}
	case status := <-statusCh:
		if status.StatusCode != 0 {
			return fmt.Errorf("job %q failed with exit code %d", job.Name, status.StatusCode)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
