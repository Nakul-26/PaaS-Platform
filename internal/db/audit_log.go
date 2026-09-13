package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AuditLogEntry is one row of the append-only audit trail
// (database-schema.md's `audit_logs` entry, phase-6-multi-tenant-saas.md
// Task 1/7). ActorUserID is nil for a system-initiated action — none exist
// yet (every action Task 7 wires this into is a human's authenticated
// request), but the column stays nullable per the schema doc for whenever
// one does.
type AuditLogEntry struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	ActorUserID *uuid.UUID
	Action      string
	TargetType  string
	TargetID    uuid.UUID
	Metadata    json.RawMessage
	CreatedAt   time.Time
}

// AuditLogRepository is the port apiserver depends on (ADR-0011). RLS-bound
// (database-schema.md §3's two-branch policy). Deliberately has no
// Update/Delete method — the table is append-only, and 0011_audit_logs.sql
// backstops that at the DB layer too (REVOKE UPDATE, DELETE FROM
// platform_app), so this interface's shape and the DB grant enforce the
// same guarantee at two independent layers.
type AuditLogRepository interface {
	Record(ctx context.Context, entry AuditLogEntry) error
	// ListByOrg returns orgID's audit log, newest first, keyset-paginated
	// on (created_at, id) — the same cursor shape api-conventions.md §4's
	// GET /v1/orgs/:orgId/audit-logs example uses. cursor is empty for the
	// first page, or a previous call's returned nextCursor. limit is
	// clamped by the caller, not here.
	ListByOrg(ctx context.Context, orgID uuid.UUID, cursor string, limit int) (entries []AuditLogEntry, nextCursor string, err error)
}

type auditLogRepository struct{ conn Conn }

func NewAuditLogRepository(conn Conn) AuditLogRepository {
	return &auditLogRepository{conn: conn}
}

func (r *auditLogRepository) Record(ctx context.Context, entry AuditLogEntry) error {
	metadata := entry.Metadata
	if metadata == nil {
		metadata = json.RawMessage(`{}`)
	}
	_, err := r.conn.Exec(ctx,
		`INSERT INTO audit_logs (org_id, actor_user_id, action, target_type, target_id, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		entry.OrgID, entry.ActorUserID, entry.Action, entry.TargetType, entry.TargetID, metadata,
	)
	if err != nil {
		return fmt.Errorf("recording audit log entry: %w", err)
	}
	return nil
}

func (r *auditLogRepository) ListByOrg(ctx context.Context, orgID uuid.UUID, cursor string, limit int) ([]AuditLogEntry, string, error) {
	const query = `
		SELECT id, org_id, actor_user_id, action, target_type, target_id, metadata, created_at
		FROM audit_logs
		WHERE org_id = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`

	var (
		afterCreatedAt *time.Time
		afterID        uuid.UUID
	)
	if cursor != "" {
		c, err := decodeAuditLogCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		afterCreatedAt, afterID = &c.CreatedAt, c.ID
	}

	// limit+1 is fetched so a next page can be detected without a separate
	// COUNT query — the same "fetch one extra row" trick every keyset
	// pagination implementation uses.
	rows, err := r.conn.Query(ctx, query, orgID, afterCreatedAt, afterID, limit+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing audit logs: %w", err)
	}
	defer rows.Close()

	var entries []AuditLogEntry
	for rows.Next() {
		var e AuditLogEntry
		if err := rows.Scan(&e.ID, &e.OrgID, &e.ActorUserID, &e.Action, &e.TargetType, &e.TargetID, &e.Metadata, &e.CreatedAt); err != nil {
			return nil, "", fmt.Errorf("scanning audit log entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating audit logs: %w", err)
	}

	var nextCursor string
	if len(entries) > limit {
		entries = entries[:limit]
		last := entries[len(entries)-1]
		nextCursor = encodeAuditLogCursor(last.CreatedAt, last.ID)
	}
	return entries, nextCursor, nil
}

// auditLogCursor is ListByOrg's opaque cursor payload — a plain
// base64url-encoded JSON object, not meant to be human-readable, only
// round-tripped by the same API that issued it (api-conventions.md §4:
// "cursor-based, not offset-based").
type auditLogCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

func encodeAuditLogCursor(createdAt time.Time, id uuid.UUID) string {
	data, _ := json.Marshal(auditLogCursor{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeAuditLogCursor(cursor string) (auditLogCursor, error) {
	var c auditLogCursor
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return auditLogCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return auditLogCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	return c, nil
}
