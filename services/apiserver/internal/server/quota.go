package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"platform/internal/db"
)

// Layer 1 of the three-layer quota scheme (ARCHITECTURE.md §2.9,
// phase-6-multi-tenant-saas.md Task 6): the API server's own fast, cheap
// pre-flight check, run inside the same transaction as the write it
// guards, so a rejection never lets a partial write through. Layer 2 (the
// scheduler, services/scheduler/internal/scheduler/placement.go) closes
// the TOCTOU gap this layer can't — two concurrent requests can both pass
// this check before either commits — and Layer 3 (a Postgres trigger,
// 0013_quota_enforcement_trigger.sql) is the last-resort backstop, expected
// to essentially never fire if layers 1/2 are correct.
//
// Every check here is called after requirePermission/requireMembership has
// already confirmed the caller belongs to orgID, so an error from
// quotas.Get/CurrentUsage below is deliberately left unwrapped (not routed
// through mapDBError): handleSignup creates a quota row in the same
// transaction as every org, so ErrNotFound here would mean the org's quota
// row is genuinely missing — a data-integrity bug, correctly a 500, not a
// misleading 404 that would suggest the caller doesn't belong to the org.

// checkProjectQuota rejects with 422 quota_exceeded if orgID is already at
// its MaxProjects ceiling — the one check project.create needs.
func checkProjectQuota(ctx context.Context, conn db.Conn, orgID uuid.UUID) error {
	quotas := db.NewQuotaRepository(conn)
	quota, err := quotas.Get(ctx, orgID)
	if err != nil {
		return err
	}
	usage, err := quotas.CurrentUsage(ctx, orgID)
	if err != nil {
		return err
	}
	if usage.Projects >= quota.MaxProjects {
		return errQuotaExceeded(fmt.Sprintf("organization is at its project limit (%d/%d)", usage.Projects, quota.MaxProjects))
	}
	return nil
}

// checkDeploymentQuota rejects with 422 quota_exceeded if orgID has already
// hit MaxDeploymentsPerDay — the deploy-specific check beyond
// checkResourceQuota below, which deploy and scale share.
func checkDeploymentQuota(ctx context.Context, conn db.Conn, orgID uuid.UUID) error {
	quotas := db.NewQuotaRepository(conn)
	quota, err := quotas.Get(ctx, orgID)
	if err != nil {
		return err
	}
	usage, err := quotas.CurrentUsage(ctx, orgID)
	if err != nil {
		return err
	}
	if usage.DeploymentsToday >= quota.MaxDeploymentsPerDay {
		return errQuotaExceeded(fmt.Sprintf("organization has reached its daily deployment limit (%d/%d in the last 24h)", usage.DeploymentsToday, quota.MaxDeploymentsPerDay))
	}
	return nil
}

// checkResourceQuota rejects with 422 quota_exceeded if growing app to
// newReplicas would push the org's total container count, CPU, or memory
// past its ceilings — shared by handleDeploy and handleScaleApplication
// ("before deploy/scale succeeds, check the resulting total container
// count and total CPU/memory"). app's *current* contribution to the org's
// live usage is subtracted out before adding back its post-action
// contribution, so a steady-state redeploy of an already-running
// application at an unchanged replica count is never double counted
// against what's already running.
//
// For containers specifically, "current contribution" is the application's
// latest deployment's actual active container count (db.ContainerRepository
// .CountActiveByDeployment), not simply app.ReplicasDesired: a freshly
// created, never-deployed application contributes zero real containers yet
// no matter what its desired count says — exactly the case a first-ever
// deploy needs this check to catch, and exactly why using ReplicasDesired
// on both sides (which would net to zero) would silently defeat the check.
//
// CPU/memory are always 0 today: no route has ever set
// applications.cpu_millicores/memory_mb past their zero default (a known,
// documented Phase 6 gap — the same category as env_vars.write never
// getting a route). The check is still real and complete, just currently
// inert on those two ceilings — same posture as every other
// ahead-of-its-route piece of this phase.
func checkResourceQuota(ctx context.Context, conn db.Conn, orgID uuid.UUID, app db.Application, newReplicas int) error {
	quotas := db.NewQuotaRepository(conn)
	quota, err := quotas.Get(ctx, orgID)
	if err != nil {
		return err
	}
	usage, err := quotas.CurrentUsage(ctx, orgID)
	if err != nil {
		return err
	}

	currentAppContainers := 0
	latest, err := db.NewDeploymentRepository(conn).Latest(ctx, app.ID)
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return err
		}
	} else {
		currentAppContainers, err = db.NewContainerRepository(conn).CountActiveByDeployment(ctx, latest.ID)
		if err != nil {
			return err
		}
	}

	resultingContainers := usage.Containers - currentAppContainers + newReplicas
	resultingCPU := usage.CPUMillicores - app.CPUMillicores*app.ReplicasDesired + app.CPUMillicores*newReplicas
	resultingMemory := usage.MemoryMB - app.MemoryMB*app.ReplicasDesired + app.MemoryMB*newReplicas

	switch {
	case resultingContainers > quota.MaxContainers:
		return errQuotaExceeded(fmt.Sprintf("this would use %d/%d containers for the organization", resultingContainers, quota.MaxContainers))
	case resultingCPU > quota.MaxCPUMillicores:
		return errQuotaExceeded(fmt.Sprintf("this would use %dm/%dm cpu for the organization", resultingCPU, quota.MaxCPUMillicores))
	case resultingMemory > quota.MaxMemoryMB:
		return errQuotaExceeded(fmt.Sprintf("this would use %dMB/%dMB memory for the organization", resultingMemory, quota.MaxMemoryMB))
	}
	return nil
}
