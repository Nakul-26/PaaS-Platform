package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"platform/internal/eventbus"
	"platform/internal/runtime"
)

// assignMessage is node.<id>.assign's payload (docs/nats-contract.md). Field
// shapes mirror worker-agent-contract.md's POST /v1/containers body, since
// the scheduler forwards a placement request into this largely unchanged.
type assignMessage struct {
	AssignmentID string            `json:"assignment_id"`
	DeploymentID string            `json:"deployment_id"`
	Image        string            `json:"image"`
	Env          map[string]string `json:"env,omitempty"`
	Ports        []portBinding     `json:"ports,omitempty"`
	Command      []string          `json:"command,omitempty"`
}

type portBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

// unassignMessage is node.<id>.unassign's payload (docs/nats-contract.md,
// phase-3-controllers.md Task 2).
type unassignMessage struct {
	AssignmentID string `json:"assignment_id"`
	ContainerID  string `json:"container_id"`
}

// statusMessage is node.<id>.status's payload. status uses containers.status'
// vocabulary (pending|running|crashed|stopped, database-schema.md §2) —
// deliberately not internal/runtime.Status's own vocabulary
// (pending|running|exited|unknown), which is worker-agent-contract.md's HTTP
// vocabulary instead. containerStatus below is the mapping between the two.
type statusMessage struct {
	AssignmentID string    `json:"assignment_id"`
	ContainerID  string    `json:"container_id"`
	Status       string    `json:"status"`
	ExitCode     int       `json:"exit_code"`
	Timestamp    time.Time `json:"timestamp"`
}

// subscribeAssignments ensures the JetStream streams this worker needs
// (docs/nats-contract.md: node.<id>.assign is consumed durably, node.<id>.status
// is published durably — loss of either is unacceptable per ADR-0005) and
// subscribes to this node's own assign subject. EnsureStream is idempotent,
// so it's safe to call here even though the scheduler (Task 5) also calls it
// when it publishes — whichever service starts first wins, harmlessly.
func (a *Agent) subscribeAssignments(ctx context.Context) (eventbus.Subscription, error) {
	if err := a.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeAssignmentsStream,
		Subjects: []string{eventbus.NodeAssignmentsStreamFilter, eventbus.NodeUnassignStreamFilter},
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.NodeAssignmentsStream, err)
	}
	if err := a.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeStatusStream,
		Subjects: []string{eventbus.NodeStatusStreamFilter},
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.NodeStatusStream, err)
	}

	nodeID := a.cfg.NodeID.String()
	consumerName := fmt.Sprintf("worker-%s-assign", nodeID)
	subject := eventbus.NodeAssignSubject(nodeID)

	return a.bus.SubscribeDurable(ctx, eventbus.NodeAssignmentsStream, consumerName, subject, func(msg eventbus.Message) error {
		a.handleAssignment(ctx, msg.Data)
		return nil
	})
}

// subscribeUnassign subscribes this node to its own node.<id>.unassign
// subject (phase-3-controllers.md Task 2). Shares the NODE_ASSIGNMENTS
// stream with node.<id>.assign (both ensured by subscribeAssignments, which
// Run always calls first) since both are assignment-lifecycle messages with
// the same loss-unacceptable reasoning (docs/nats-contract.md).
func (a *Agent) subscribeUnassign(ctx context.Context) (eventbus.Subscription, error) {
	nodeID := a.cfg.NodeID.String()
	consumerName := fmt.Sprintf("worker-%s-unassign", nodeID)
	subject := eventbus.NodeUnassignSubject(nodeID)

	return a.bus.SubscribeDurable(ctx, eventbus.NodeAssignmentsStream, consumerName, subject, func(msg eventbus.Message) error {
		a.handleUnassign(ctx, msg.Data)
		return nil
	})
}

// handleAssignment drives the same ContainerRuntime start path the HTTP
// POST /v1/containers handler uses (services/worker/internal/server), then
// publishes the outcome on node.<id>.status. Errors are reported as a
// "crashed" status, not by requesting NATS redelivery (see subscribeAssignments'
// caller) — retrying a bad image name or a rejected spec wouldn't help.
func (a *Agent) handleAssignment(ctx context.Context, data []byte) {
	var msg assignMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		a.logger.Error("decoding assignment message", "error", err)
		return
	}

	info, err := a.startAssignedContainer(ctx, msg)
	if err != nil {
		a.logger.Error("acting on assignment", "assignment_id", msg.AssignmentID, "image", msg.Image, "error", err)
		a.publishStatus(ctx, msg.AssignmentID, "", "crashed", 0)
		return
	}

	status := containerStatus(info)
	a.logger.Info("assignment started", "assignment_id", msg.AssignmentID, "container_id", info.ID, "image", msg.Image)
	a.publishStatus(ctx, msg.AssignmentID, info.ID, status, info.ExitCode)

	a.mu.Lock()
	a.tracked[msg.AssignmentID] = &trackedContainer{containerID: info.ID, lastStatus: status}
	a.mu.Unlock()
}

// handleUnassign stops and removes one specific container
// (docs/nats-contract.md's node.<id>.unassign, phase-3-controllers.md Task 2)
// — the mechanism that lets anything (controller, apiserver) act on a
// container wherever it actually runs, instead of a hardcoded worker address.
// Stop/remove errors are logged, not treated as fatal to this handler: an
// unassign racing a crash that Task 1's poll already caught (container
// already stopped/gone) should still converge to a final "stopped" status
// and stop tracking, not get stuck retrying.
func (a *Agent) handleUnassign(ctx context.Context, data []byte) {
	var msg unassignMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		a.logger.Error("decoding unassign message", "error", err)
		return
	}

	if err := a.rt.StopContainer(ctx, msg.ContainerID, 10*time.Second); err != nil {
		a.logger.Warn("stopping container for unassign", "assignment_id", msg.AssignmentID, "container_id", msg.ContainerID, "error", err)
	}
	if err := a.rt.RemoveContainer(ctx, msg.ContainerID); err != nil {
		a.logger.Warn("removing container for unassign", "assignment_id", msg.AssignmentID, "container_id", msg.ContainerID, "error", err)
	}

	a.logger.Info("container unassigned", "assignment_id", msg.AssignmentID, "container_id", msg.ContainerID)
	a.publishStatus(ctx, msg.AssignmentID, msg.ContainerID, "stopped", 0)

	a.mu.Lock()
	delete(a.tracked, msg.AssignmentID)
	a.mu.Unlock()
}

// checkTrackedContainers polls every container this worker started
// (phase-3-controllers.md Task 1) and republishes node.<id>.status only when
// the observed status has changed since the last check — a container killed
// out-of-band (docker kill, not the worker process) is otherwise never
// noticed, since handleAssignment only publishes once, at start. Recovering
// tracking after a worker *process* restart is explicitly out of scope (the
// map is in-memory only; see phase-3-controllers.md's "Explicitly deferred").
func (a *Agent) checkTrackedContainers(ctx context.Context) {
	a.mu.Lock()
	snapshot := make(map[string]trackedContainer, len(a.tracked))
	for assignmentID, tc := range a.tracked {
		snapshot[assignmentID] = *tc
	}
	a.mu.Unlock()

	for assignmentID, tc := range snapshot {
		info, err := a.rt.ContainerStatus(ctx, tc.containerID)
		if err != nil {
			a.logger.Error("checking tracked container health", "assignment_id", assignmentID, "container_id", tc.containerID, "error", err)
			continue
		}

		status := containerStatus(info)
		if status == tc.lastStatus {
			continue
		}

		a.logger.Info("tracked container status changed", "assignment_id", assignmentID, "container_id", tc.containerID, "from", tc.lastStatus, "to", status)
		a.publishStatus(ctx, assignmentID, tc.containerID, status, info.ExitCode)

		a.mu.Lock()
		if current, ok := a.tracked[assignmentID]; ok {
			current.lastStatus = status
		}
		a.mu.Unlock()
	}
}

func (a *Agent) startAssignedContainer(ctx context.Context, msg assignMessage) (runtime.ContainerStatusInfo, error) {
	if err := a.rt.PullImage(ctx, msg.Image); err != nil {
		return runtime.ContainerStatusInfo{}, fmt.Errorf("pulling image %s: %w", msg.Image, err)
	}

	spec := runtime.ContainerSpec{
		Image:   msg.Image,
		Env:     msg.Env,
		Command: msg.Command,
	}
	for _, p := range msg.Ports {
		spec.Ports = append(spec.Ports, runtime.PortBinding{
			ContainerPort: p.ContainerPort,
			HostPort:      p.HostPort,
			Protocol:      p.Protocol,
		})
	}

	id, err := a.rt.CreateContainer(ctx, spec)
	if err != nil {
		return runtime.ContainerStatusInfo{}, fmt.Errorf("creating container: %w", err)
	}
	if err := a.rt.StartContainer(ctx, id); err != nil {
		return runtime.ContainerStatusInfo{}, fmt.Errorf("starting container %s: %w", id, err)
	}

	info, err := a.rt.ContainerStatus(ctx, id)
	if err != nil {
		return runtime.ContainerStatusInfo{}, fmt.Errorf("inspecting container %s: %w", id, err)
	}
	return info, nil
}

func (a *Agent) publishStatus(ctx context.Context, assignmentID, containerID, status string, exitCode int) {
	data, err := json.Marshal(statusMessage{
		AssignmentID: assignmentID,
		ContainerID:  containerID,
		Status:       status,
		ExitCode:     exitCode,
		Timestamp:    time.Now().UTC(),
	})
	if err != nil {
		a.logger.Error("marshaling status message", "assignment_id", assignmentID, "error", err)
		return
	}

	nodeID := a.cfg.NodeID.String()
	if err := a.bus.PublishDurable(ctx, eventbus.NodeStatusSubject(nodeID), data); err != nil {
		a.logger.Error("publishing status message", "assignment_id", assignmentID, "error", err)
	}
}

// containerStatus maps internal/runtime.Status (the HTTP contract's
// vocabulary) onto containers.status (the NATS/DB contract's vocabulary,
// database-schema.md §2). An exited container is "stopped" if it exited
// cleanly (code 0) and "crashed" otherwise — runtime.Status alone can't
// distinguish the two, only the exit code can.
func containerStatus(info runtime.ContainerStatusInfo) string {
	switch info.Status {
	case runtime.StatusPending:
		return "pending"
	case runtime.StatusRunning:
		return "running"
	case runtime.StatusExited:
		if info.ExitCode == 0 {
			return "stopped"
		}
		return "crashed"
	default:
		return "crashed"
	}
}
