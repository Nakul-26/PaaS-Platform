// Package imagebuilder implements image-builder's whole job (ARCHITECTURE.md
// §2.10, phase-7-deployment-platform.md): clone a Git repo, build a Docker
// image from a Dockerfile at its root, push it to a registry, and report
// the outcome. Named imagebuilder rather than the modularity registry
// row's literal bare "internal" to match every sibling service's own
// internal/<name> subpackage convention (services/scheduler/internal/
// scheduler, services/worker/internal/nodeagent, etc.) — same package tree
// the registry row points at, just one level more specific.
package imagebuilder

import (
	"context"
	"time"
)

// CloneTimeout/BuildTimeout are placeholder ceilings on how long a single
// build attempt is allowed to clone/build+push for — untuned constants,
// same "revisit with real usage data" posture every other placeholder in
// this codebase carries (e.g. phase-6-multi-tenant-saas.md's
// DefaultResourceQuota).
const (
	CloneTimeout = 2 * time.Minute
	BuildTimeout = 10 * time.Minute
)

// Request is one build attempt's input — ImageRepository is the target
// repository *without* a tag (e.g. "registry:5000/<org_id>/<application_id>",
// computed by apiserver per phase-7-deployment-platform.md Open Decision
// 3); Build itself appends ":<commit_sha>" once the clone resolves it,
// since the SHA isn't known until after cloning.
type Request struct {
	GitURL          string
	GitRef          string
	ImageRepository string
}

// Result is a successful build's output — the exact image reference
// (repository:commit_sha) a subsequent deploy can pull.
type Result struct {
	CommitSHA string
	Image     string
}

// ImageBuilder is the port image-builder's NATS consumer depends on
// (ADR-0011). DockerBuilder (docker.go) is the only adapter today;
// Buildpacks/Nixpacks/Kaniko are docs/modularity-and-extensibility.md's
// documented future alternatives.
type ImageBuilder interface {
	// Build clones req.GitURL at req.GitRef, builds a Docker image from
	// the Dockerfile at its root, tags it req.ImageRepository:<commit-sha>,
	// and pushes it. A non-nil error means req's build.completed outcome
	// is "failed" with error.Error() as the reason — every failure mode
	// (bad ref, missing Dockerfile, build failure, push failure) is
	// reported the same way, there is no partial-success case.
	Build(ctx context.Context, req Request) (Result, error)
}
