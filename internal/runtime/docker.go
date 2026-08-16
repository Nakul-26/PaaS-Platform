package runtime

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// DockerRuntime implements ContainerRuntime against a local Docker Engine
// daemon via the official Docker Go SDK (ADR-0006). It is the only package
// in this module allowed to import the Docker client directly (ADR-0011).
type DockerRuntime struct {
	cli *client.Client
}

var _ ContainerRuntime = (*DockerRuntime)(nil)

// NewDockerRuntime connects to the Docker daemon using the standard
// environment configuration (DOCKER_HOST, etc., falling back to the local
// socket/pipe).
func NewDockerRuntime() (*DockerRuntime, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("connecting to docker daemon: %w", err)
	}
	return &DockerRuntime{cli: cli}, nil
}

// Close releases the underlying Docker client connection.
func (d *DockerRuntime) Close() error {
	return d.cli.Close()
}

func (d *DockerRuntime) PullImage(ctx context.Context, image string) error {
	resp, err := d.cli.ImagePull(ctx, image, client.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("pulling image %s: %w", image, err)
	}
	defer func() { _ = resp.Close() }()
	// The pull response must be fully drained for the pull to complete.
	if _, err := io.Copy(io.Discard, resp); err != nil {
		return fmt.Errorf("reading pull response for image %s: %w", image, err)
	}
	return nil
}

func (d *DockerRuntime) CreateContainer(ctx context.Context, spec ContainerSpec) (string, error) {
	env := make([]string, 0, len(spec.Env))
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	exposedPorts := make(network.PortSet, len(spec.Ports))
	portBindings := make(network.PortMap, len(spec.Ports))
	unspecifiedIP := netip.IPv4Unspecified()
	for _, p := range spec.Ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		containerPort, err := network.ParsePort(fmt.Sprintf("%d/%s", p.ContainerPort, proto))
		if err != nil {
			return "", fmt.Errorf("building port spec for %d/%s: %w", p.ContainerPort, proto, err)
		}
		exposedPorts[containerPort] = struct{}{}
		hostPort := ""
		if p.HostPort != 0 {
			hostPort = fmt.Sprintf("%d", p.HostPort)
		}
		portBindings[containerPort] = []network.PortBinding{{HostIP: unspecifiedIP, HostPort: hostPort}}
	}

	result, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Image: spec.Image,
		Name:  spec.Name,
		Config: &container.Config{
			Env:          env,
			Cmd:          spec.Command,
			ExposedPorts: exposedPorts,
		},
		HostConfig: &container.HostConfig{
			PortBindings: portBindings,
		},
	})
	if err != nil {
		return "", fmt.Errorf("creating container from image %s: %w", spec.Image, err)
	}
	return result.ID, nil
}

func (d *DockerRuntime) StartContainer(ctx context.Context, containerID string) error {
	if _, err := d.cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("starting container %s: %w", containerID, err)
	}
	return nil
}

func (d *DockerRuntime) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	seconds := int(timeout.Seconds())
	if _, err := d.cli.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &seconds}); err != nil {
		return fmt.Errorf("stopping container %s: %w", containerID, err)
	}
	return nil
}

func (d *DockerRuntime) RemoveContainer(ctx context.Context, containerID string) error {
	if _, err := d.cli.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("removing container %s: %w", containerID, err)
	}
	return nil
}

func (d *DockerRuntime) ContainerStatus(ctx context.Context, containerID string) (ContainerStatusInfo, error) {
	result, err := d.cli.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return ContainerStatusInfo{}, fmt.Errorf("inspecting container %s: %w", containerID, err)
	}
	inspect := result.Container

	status := StatusUnknown
	if inspect.State != nil {
		switch inspect.State.Status {
		case container.StateCreated:
			status = StatusPending
		case container.StateRunning:
			status = StatusRunning
		case container.StateExited, container.StateDead:
			status = StatusExited
		}
	}

	var (
		exitCode  int
		startedAt time.Time
	)
	if inspect.State != nil {
		exitCode = inspect.State.ExitCode
		startedAt, _ = time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
	}

	return ContainerStatusInfo{
		ID:        inspect.ID,
		Status:    status,
		ExitCode:  exitCode,
		StartedAt: startedAt,
	}, nil
}

func (d *DockerRuntime) StreamLogs(ctx context.Context, containerID string, follow bool) (io.ReadCloser, error) {
	raw, err := d.cli.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
	})
	if err != nil {
		return nil, fmt.Errorf("streaming logs for container %s: %w", containerID, err)
	}

	// Docker multiplexes stdout/stderr into a custom frame format on this
	// stream; demultiplex it here so StreamLogs' contract is a plain byte
	// stream regardless of the underlying runtime's wire format (ADR-0011).
	pr, pw := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(pw, pw, raw)
		_ = raw.Close()
		_ = pw.CloseWithError(err)
	}()

	return pr, nil
}
