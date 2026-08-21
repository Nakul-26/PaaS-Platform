package server

// placementRequestedMessage/portBinding mirror docs/nats-contract.md's
// placement.requested schema — apiserver's own copy, since per ADR-0012 it
// may depend only on the published NATS contract, never the scheduler's
// package tree (the same reasoning services/scheduler's own copies of these
// shapes already documents).
type placementRequestedMessage struct {
	DeploymentID  string            `json:"deployment_id"`
	ApplicationID string            `json:"application_id"`
	Image         string            `json:"image"`
	Env           map[string]string `json:"env,omitempty"`
	Ports         []portBinding     `json:"ports,omitempty"`
	Command       []string          `json:"command,omitempty"`
}

type portBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}
