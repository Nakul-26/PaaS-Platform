package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// BuildStatus mirrors the build_status Postgres enum (0014_builds.sql),
// same ContainerStatus/DeploymentStatus-style typed-string convention.
type BuildStatus string

const (
	BuildStatusPending   BuildStatus = "pending"
	BuildStatusCloning   BuildStatus = "cloning"
	BuildStatusBuilding  BuildStatus = "building"
	BuildStatusPushing   BuildStatus = "pushing"
	BuildStatusSucceeded BuildStatus = "succeeded"
	BuildStatusFailed    BuildStatus = "failed"
)

// Build is one git-based deploy attempt (phase-7-deployment-platform.md
// Task 1) — CommitSHA/Image/ErrorMessage are nil until image-builder
// (Task 3) reports an outcome via UpdateResult.
type Build struct {
	ID            uuid.UUID
	OrgID         uuid.UUID
	ApplicationID uuid.UUID
	GitURL        string
	GitRef        string
	CommitSHA     *string
	Status        BuildStatus
	Image         *string
	ErrorMessage  *string
	CreatedBy     uuid.UUID
	CreatedAt     time.Time
	CompletedAt   *time.Time
}

// BuildRepository is the port apiserver depends on (ADR-0011). RLS-bound
// (database-schema.md §3's two-branch policy, same shape as
// DomainRepository) — every method here runs over the RLS-scoped
// platform_app connection; image-builder itself never touches this
// repository at all (ADR-0012 — it only speaks NATS, per
// phase-7-deployment-platform.md Task 3/4).
type BuildRepository interface {
	// Create inserts a new build request, status='pending'. gitRef
	// defaults to 'main' at the DB layer if empty is passed through.
	Create(ctx context.Context, orgID, applicationID uuid.UUID, gitURL, gitRef string, createdBy uuid.UUID) (Build, error)
	// Get returns one build by id. Added for apiserver's build.completed
	// consumer (phase-7-deployment-platform.md Task 4): a NATS message
	// carries only a build id, and every value the consumer acts on
	// (application_id, created_by, org_id) has to come off the row itself
	// rather than the message, so a forged payload fails closed against
	// RLS instead of reaching another tenant's data.
	Get(ctx context.Context, id uuid.UUID) (Build, error)
	// ListByApplication returns builds newest-first, cursor-paginated per
	// api-conventions.md §4 (same (created_at, id) keyset shape
	// AuditLogRepository.ListByOrg uses). cursor is empty for the first
	// page.
	ListByApplication(ctx context.Context, applicationID uuid.UUID, limit int, cursor string) (builds []Build, nextCursor string, err error)
	// OrgID resolves id's owning org — see ProjectRepository.OrgID for the
	// usual reason (a deep-by-ID route with no orgId in its URL).
	OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
	// UpdateResult records image-builder's reported outcome (Task 3/4): a
	// successful build carries commitSHA/image and no errMsg; a failed one
	// carries errMsg and no commitSHA/image. status must be 'succeeded' or
	// 'failed' — this is always the terminal write for a build.
	UpdateResult(ctx context.Context, id uuid.UUID, status BuildStatus, commitSHA, image, errMsg *string) error
}

type buildRepository struct{ conn Conn }

func NewBuildRepository(conn Conn) BuildRepository {
	return &buildRepository{conn: conn}
}

const buildColumns = `id, org_id, application_id, git_url, git_ref, commit_sha, status, image, error_message, created_by, created_at, completed_at`

func scanBuild(row pgx.Row) (Build, error) {
	var b Build
	err := row.Scan(&b.ID, &b.OrgID, &b.ApplicationID, &b.GitURL, &b.GitRef, &b.CommitSHA, &b.Status, &b.Image, &b.ErrorMessage, &b.CreatedBy, &b.CreatedAt, &b.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Build{}, ErrNotFound
		}
		return Build{}, err
	}
	return b, nil
}

func (r *buildRepository) Create(ctx context.Context, orgID, applicationID uuid.UUID, gitURL, gitRef string, createdBy uuid.UUID) (Build, error) {
	if gitRef == "" {
		gitRef = "main"
	}
	b, err := scanBuild(r.conn.QueryRow(ctx,
		`INSERT INTO builds (org_id, application_id, git_url, git_ref, created_by)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING `+buildColumns,
		orgID, applicationID, gitURL, gitRef, createdBy,
	))
	if err != nil {
		return Build{}, fmt.Errorf("creating build: %w", err)
	}
	return b, nil
}

func (r *buildRepository) Get(ctx context.Context, id uuid.UUID) (Build, error) {
	b, err := scanBuild(r.conn.QueryRow(ctx, `SELECT `+buildColumns+` FROM builds WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Build{}, ErrNotFound
		}
		return Build{}, fmt.Errorf("getting build: %w", err)
	}
	return b, nil
}

func (r *buildRepository) ListByApplication(ctx context.Context, applicationID uuid.UUID, limit int, cursor string) ([]Build, string, error) {
	var (
		afterCreatedAt *time.Time
		afterID        uuid.UUID
	)
	if cursor != "" {
		c, err := decodeBuildCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		afterCreatedAt, afterID = &c.CreatedAt, c.ID
	}

	// limit+1 is fetched so a next page can be detected without a separate
	// COUNT query — same trick AuditLogRepository.ListByOrg uses.
	rows, err := r.conn.Query(ctx,
		`SELECT `+buildColumns+` FROM builds
		 WHERE application_id = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $4`,
		applicationID, afterCreatedAt, afterID, limit+1,
	)
	if err != nil {
		return nil, "", fmt.Errorf("listing builds: %w", err)
	}
	defer rows.Close()

	var builds []Build
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scanning build: %w", err)
		}
		builds = append(builds, b)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating builds: %w", err)
	}

	var nextCursor string
	if len(builds) > limit {
		builds = builds[:limit]
		last := builds[len(builds)-1]
		nextCursor = encodeBuildCursor(last.CreatedAt, last.ID)
	}
	return builds, nextCursor, nil
}

func (r *buildRepository) OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var orgID uuid.UUID
	err := r.conn.QueryRow(ctx, `SELECT org_id FROM builds WHERE id = $1`, id).Scan(&orgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, fmt.Errorf("resolving build org: %w", err)
	}
	return orgID, nil
}

func (r *buildRepository) UpdateResult(ctx context.Context, id uuid.UUID, status BuildStatus, commitSHA, image, errMsg *string) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE builds SET status = $2, commit_sha = $3, image = $4, error_message = $5, completed_at = now() WHERE id = $1`,
		id, status, commitSHA, image, errMsg,
	)
	if err != nil {
		return fmt.Errorf("updating build result: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// buildCursor is ListByApplication's opaque cursor payload — same
// base64url-encoded-JSON shape as auditLogCursor.
type buildCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

func encodeBuildCursor(createdAt time.Time, id uuid.UUID) string {
	data, _ := json.Marshal(buildCursor{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeBuildCursor(cursor string) (buildCursor, error) {
	var c buildCursor
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return buildCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return buildCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	return c, nil
}
