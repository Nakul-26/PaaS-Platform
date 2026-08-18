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

func NodeStatusSubject(nodeID string) string {
	return fmt.Sprintf("node.%s.status", nodeID)
}

// Stream names and wildcard subject filters for the JetStream-backed
// subjects (docs/nats-contract.md's transport table) — assignments and
// status transitions are loss-unacceptable, unlike register/heartbeat above.
const (
	NodeAssignmentsStream       = "NODE_ASSIGNMENTS"
	NodeAssignmentsStreamFilter = "node.*.assign"

	NodeStatusStream       = "NODE_STATUS"
	NodeStatusStreamFilter = "node.*.status"
)
