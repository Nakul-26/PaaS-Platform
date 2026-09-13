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

// ServiceUpdatedSubject is published by controller-manager's Service
// Instance controller whenever a service_instances row changes
// (phase-4-service-discovery-lb.md Task 3). Core NATS, not JetStream —
// ARCHITECTURE.md §2.6's own reasoning: an occasional missed event is
// acceptable since Task 4's periodic full-Postgres-resync pairs with it.
const ServiceUpdatedSubject = "service.updated"

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

// BuildRequestedSubject is published by apiserver when an application is
// deployed from a Git repo URL instead of an already-pushed image
// (phase-7-deployment-platform.md Task 3/4, docs/nats-contract.md).
// BuildCompletedSubject is published by image-builder once a build
// attempt reaches a terminal outcome — one subject for both success and
// failure (payload carries a status field), mirroring node.<id>.status's
// existing "one subject, a status field carries the branch" precedent
// rather than splitting into two subjects. Both are JetStream-backed on
// one BUILDS stream: a lost build.requested strands a deploy attempt with
// nothing to retry it, and a lost build.completed leaves a build stuck in
// 'pending' forever — the same "unacceptable loss" reasoning
// placement.requested/node.<id>.status already establish.
const (
	BuildRequestedSubject = "build.requested"
	BuildCompletedSubject = "build.completed"

	BuildsStream = "BUILDS"
)

// BuildsStreamSubjects is BuildsStream's subject list, passed to
// EnsureStream.
var BuildsStreamSubjects = []string{BuildRequestedSubject, BuildCompletedSubject}
