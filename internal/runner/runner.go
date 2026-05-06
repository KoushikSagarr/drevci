package runner

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/internal/workspace"
	"github.com/drevci/drev/pkg/drevtypes"
)

// Runner handles the execution of jobs in Docker containers.
type Runner struct {
	docker *client.Client
	store  store.Store
}

type JobResult struct {
	JobName  string
	ExitCode int
	Error    error
	Duration time.Duration
}

func New(store store.Store) (*Runner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("creating docker client: %w", err)
	}
	return &Runner{
		docker: cli,
		store:  store,
		}, nil
}

func (r *Runner) RunJob(ctx context.Context, run *drevtypes.Run, job *drevtypes.Job, w *workspace.Workspace, logWriter io.Writer) error {
	// Helper to log errors to the dashboard before returning them
	logErr := func(msg string, err error) error {
		wrapped := fmt.Errorf("%s: %w", msg, err)
		fmt.Fprintf(logWriter, "[drev] ✗ ERROR: %s\n", wrapped)
		return wrapped
	}

	// Check if image exists locally to save time
	_, _, err := r.docker.ImageInspectWithRaw(ctx, job.Image)
	if err != nil {
		fmt.Fprintf(logWriter, "[drev] image not found locally, pulling: %s\n", job.Image)
		reader, err := r.docker.ImagePull(ctx, job.Image, image.PullOptions{})
		if err != nil {
			return logErr("pulling image", err)
		}
		io.Copy(logWriter, reader)
		reader.Close()
	} else {
		fmt.Fprintf(logWriter, "[drev] using cached image: %s\n", job.Image)
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

	resp, err := r.docker.ContainerCreate(ctx, &container.Config{
		Image:      job.Image,
		Cmd:        []string{"sh", "-c", shCmd},
		WorkingDir: "/workspace",
		Env:        envVars,
	}, &container.HostConfig{
		Binds: []string{w.Dir + ":/workspace"},
	}, nil, nil, "")
	if err != nil {
		return logErr("creating container", err)
	}

	containerID := resp.ID
	defer func() {
		r.docker.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
	}()

	if err := r.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return logErr("starting container", err)
	}

	logsReader, err := r.docker.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return logErr("getting container logs", err)
	}
	defer logsReader.Close()

	// Use stdcopy to demultiplex stdout/stderr properly
	_, err = stdcopy.StdCopy(logWriter, logWriter, logsReader)
	if err != nil {
		return logErr("streaming logs", err)
	}

	statusCh, errCh := r.docker.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return logErr("waiting for container", err)
		}
	case status := <-statusCh:
		if status.StatusCode != 0 {
			err := fmt.Errorf("job %q failed with exit code %d", job.Name, status.StatusCode)
			fmt.Fprintf(logWriter, "[drev] ✗ %s\n", err)
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
