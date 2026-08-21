package eventbus

import "fmt"

// NodeRegisterSubject and NodeHeartbeatSubject build the per-node subjects
// docs/nats-contract.md defines. Centralized here (rather than formatted
// ad hoc in each publisher/consumer) so the worker's publisher and the
// scheduler's consumer (Task 5) can't drift apart on the exact string.
func NodeRegisterSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.register", nodeID)
}

func NodeHeartbeatSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.heartbeat", nodeID)
}

func NodeAssignSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.assign", nodeID)
}

// NodeUnassignSubject builds the subject used to tell a specific node to
// stop and remove one specific container (docs/nats-contract.md,
// phase-3-controllers.md Task 2) — the mechanism that lets anything
// (controller, apiserver) act on a container wherever it actually runs,
// instead of a hardcoded worker address.
func NodeUnassignSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.unassign", nodeID)
}

func NodeStatusSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.status", nodeID)
}

// PlacementRequestedSubject is the fixed (non-per-node) subject the API
// server publishes to when a deployment needs scheduling
// (docs/nats-contract.md).
const PlacementRequestedSubject = "placement.requested"

// Stream names and wildcard subject filters for the JetStream-backed
// subjects (docs/nats-contract.md's transport table) — assignments and
// status transitions are loss-unacceptable, unlike register/heartbeat above.
const (
	NodeAssignmentsStream       = "NODE_ASSIGNMENTS"
	NodeAssignmentsStreamFilter = "node.*.assign"
	NodeUnassignStreamFilter    = "node.*.unassign"

	NodeStatusStream       = "NODE_STATUS"
	NodeStatusStreamFilter = "node.*.status"

	PlacementStream       = "PLACEMENT"
	PlacementStreamFilter = "placement.requested"
)
