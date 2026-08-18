package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type NodeStatus string

const (
	NodeStatusHealthy     NodeStatus = "healthy"
	NodeStatusDegraded    NodeStatus = "degraded"
	NodeStatusUnreachable NodeStatus = "unreachable"
)

type Node struct {
	ID                    uuid.UUID
	Hostname              string
	IP                    string
	CPUCapacityMillicores int
	MemoryCapacityMB      int
	Status                NodeStatus
	LastHeartbeatAt       time.Time
	Labels                map[string]string
	CreatedAt             time.Time
}

// NodeRepository is the port apiserver/scheduler depend on (ADR-0011).
// nodes carries no RLS (database-schema.md's `nodes` entry — infrastructure,
// not tenant-scoped) so its methods work against any Conn.
type NodeRepository interface {
	// Register upserts a node row keyed by id, the UUID a worker generates
	// and persists locally on first run (docs/nats-contract.md's
	// node.<id>.register) so restarts re-register the same node instead of
	// leaking a new row every restart. Re-registration also revives a
	// previously-unreachable node back to healthy.
	Register(ctx context.Context, id uuid.UUID, hostname, ip string, cpuCapacityMillicores, memoryCapacityMB int, labels map[string]string) (Node, error)
	// Heartbeat updates last_heartbeat_at and revives a previously
	// unreachable node back to healthy (docs/nats-contract.md:
	// node.<id>.heartbeat re-establishes the node).
	Heartbeat(ctx context.Context, id uuid.UUID, timestamp time.Time) error
	// SetStatus is used by the scheduler's liveness sweep to mark a node
	// unreachable once its last_heartbeat_at exceeds the configurable
	// timeout (phase-2-multi-node.md Task 5).
	SetStatus(ctx context.Context, id uuid.UUID, status NodeStatus) error
	Get(ctx context.Context, id uuid.UUID) (Node, error)
	// List returns every node, newest-first — backs GET /v1/nodes (Task 6)
	// and the scheduler's placement filter (Task 5).
	List(ctx context.Context) ([]Node, error)
}

type nodeRepository struct{ conn Conn }

func NewNodeRepository(conn Conn) NodeRepository {
	return &nodeRepository{conn: conn}
}

const nodeColumns = `id, hostname, ip, cpu_capacity_millicores, memory_capacity_mb, status, last_heartbeat_at, labels, created_at`

func scanNode(row pgx.Row) (Node, error) {
	var n Node
	var labelsRaw []byte
	err := row.Scan(&n.ID, &n.Hostname, &n.IP, &n.CPUCapacityMillicores, &n.MemoryCapacityMB, &n.Status, &n.LastHeartbeatAt, &labelsRaw, &n.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Node{}, ErrNotFound
		}
		return Node{}, err
	}
	if err := json.Unmarshal(labelsRaw, &n.Labels); err != nil {
		return Node{}, fmt.Errorf("unmarshaling labels: %w", err)
	}
	return n, nil
}

func (r *nodeRepository) Register(ctx context.Context, id uuid.UUID, hostname, ip string, cpuCapacityMillicores, memoryCapacityMB int, labels map[string]string) (Node, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return Node{}, fmt.Errorf("marshaling labels: %w", err)
	}

	n, err := scanNode(r.conn.QueryRow(ctx,
		`INSERT INTO nodes (id, hostname, ip, cpu_capacity_millicores, memory_capacity_mb, labels, last_heartbeat_at)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb, now())
		 ON CONFLICT (id) DO UPDATE SET
		     hostname = EXCLUDED.hostname,
		     ip = EXCLUDED.ip,
		     cpu_capacity_millicores = EXCLUDED.cpu_capacity_millicores,
		     memory_capacity_mb = EXCLUDED.memory_capacity_mb,
		     labels = EXCLUDED.labels,
		     last_heartbeat_at = now(),
		     status = 'healthy'
		 RETURNING `+nodeColumns,
		id, hostname, ip, cpuCapacityMillicores, memoryCapacityMB, string(labelsJSON),
	))
	if err != nil {
		return Node{}, fmt.Errorf("registering node: %w", err)
	}
	return n, nil
}

func (r *nodeRepository) Heartbeat(ctx context.Context, id uuid.UUID, timestamp time.Time) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE nodes SET last_heartbeat_at = $2, status = 'healthy' WHERE id = $1`,
		id, timestamp,
	)
	if err != nil {
		return fmt.Errorf("recording node heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *nodeRepository) SetStatus(ctx context.Context, id uuid.UUID, status NodeStatus) error {
	tag, err := r.conn.Exec(ctx, `UPDATE nodes SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("setting node status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *nodeRepository) Get(ctx context.Context, id uuid.UUID) (Node, error) {
	return scanNode(r.conn.QueryRow(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id = $1`, id))
}

func (r *nodeRepository) List(ctx context.Context) ([]Node, error) {
	rows, err := r.conn.Query(ctx, `SELECT `+nodeColumns+` FROM nodes ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning node: %w", err)
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating nodes: %w", err)
	}
	return nodes, nil
}
