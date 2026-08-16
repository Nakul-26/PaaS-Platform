package runtime

import (
	"context"
	"io"
	"time"
)

// Status is the lifecycle state of a container, independent of any
// particular runtime's own status vocabulary.
type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusExited  Status = "exited"
	StatusUnknown Status = "unknown"
)

// PortBinding maps a container port to a host port. HostPort of 0 means the
// runtime chooses an ephemeral host port.
type PortBinding struct {
	ContainerPort int
	HostPort      int
	Protocol      string // "tcp" or "udp"; empty defaults to "tcp"
}

// ContainerSpec is the minimal set of inputs the worker agent needs to start
// a container — deliberately not a pass-through of any single runtime SDK's
// full config surface (ADR-0011).
type ContainerSpec struct {
	Image   string
	Name    string
	Env     map[string]string
	Ports   []PortBinding
	Command []string
}

// ContainerStatusInfo is the observed state of a container.
type ContainerStatusInfo struct {
	ID        string
	Status    Status
	ExitCode  int
	StartedAt time.Time
}

// ContainerRuntime is the port through which the worker agent manages
// containers, independent of the underlying engine (ADR-0006). Docker is the
// first adapter; containerd/Firecracker are anticipated future ones
// (docs/modularity-and-extensibility.md §2).
type ContainerRuntime interface {
	PullImage(ctx context.Context, image string) error
	CreateContainer(ctx context.Context, spec ContainerSpec) (containerID string, err error)
	StartContainer(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string, timeout time.Duration) error
	RemoveContainer(ctx context.Context, containerID string) error
	ContainerStatus(ctx context.Context, containerID string) (ContainerStatusInfo, error)
	// StreamLogs returns a plain (already demultiplexed) stream of combined
	// stdout/stderr log bytes. The caller must Close it.
	StreamLogs(ctx context.Context, containerID string, follow bool) (io.ReadCloser, error)
}
