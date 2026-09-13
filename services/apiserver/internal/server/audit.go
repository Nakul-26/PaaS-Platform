package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"platform/internal/db"
)

// recordAudit writes one audit_logs row (phase-6-multi-tenant-saas.md
// Task 7) over the same conn — and therefore inside the same transaction —
// as the mutation it records, per that task's open decision 4: synchronous
// and in-transaction, not NATS-subscriber-driven, so there's no risk of a
// lost/never-consumed event silently producing an incomplete audit trail.
// actorUserID is passed by value, never nil, here — every call site is an
// authenticated human request (AuditLogEntry.ActorUserID stays nullable for
// a system-initiated action, but none exists yet). metadata may be nil.
func recordAudit(ctx context.Context, conn db.Conn, orgID, actorUserID uuid.UUID, action, targetType string, targetID uuid.UUID, metadata map[string]any) error {
	var raw json.RawMessage
	if len(metadata) > 0 {
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshaling audit log metadata: %w", err)
		}
		raw = b
	}
	return db.NewAuditLogRepository(conn).Record(ctx, db.AuditLogEntry{
		OrgID:       orgID,
		ActorUserID: &actorUserID,
		Action:      action,
		TargetType:  targetType,
		TargetID:    targetID,
		Metadata:    raw,
	})
}
