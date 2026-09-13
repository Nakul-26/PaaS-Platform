package imagebuilder

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/moby/moby/client"
)

// DockerBuilder implements ImageBuilder against a local Docker Engine
// daemon via the official Go SDK — the only file in this package allowed
// to import the Docker client directly (ADR-0011, same discipline
// internal/runtime.DockerRuntime already established for the worker).
type DockerBuilder struct {
	cli *client.Client
}

var _ ImageBuilder = (*DockerBuilder)(nil)

// NewDockerBuilder connects to the Docker daemon using the standard
// environment configuration (DOCKER_HOST, etc., falling back to the local
// socket/pipe) — same connection convention runtime.NewDockerRuntime uses.
func NewDockerBuilder() (*DockerBuilder, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("connecting to docker daemon: %w", err)
	}
	return &DockerBuilder{cli: cli}, nil
}

// Close releases the underlying Docker client connection.
func (b *DockerBuilder) Close() error {
	return b.cli.Close()
}

func (b *DockerBuilder) Build(ctx context.Context, req Request) (Result, error) {
	dir, commitSHA, err := cloneRepo(ctx, req.GitURL, req.GitRef, CloneTimeout)
	if err != nil {
		return Result{}, fmt.Errorf("cloning %s@%s: %w", req.GitURL, req.GitRef, err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	tag := req.ImageRepository + ":" + commitSHA

	buildCtx, err := tarDirectory(dir)
	if err != nil {
		return Result{}, fmt.Errorf("preparing build context: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, BuildTimeout)
	defer cancel()

	buildResult, err := b.cli.ImageBuild(timeoutCtx, buildCtx, client.ImageBuildOptions{
		Tags:       []string{tag},
		Dockerfile: "Dockerfile",
		Remove:     true,
	})
	if err != nil {
		return Result{}, fmt.Errorf("building image: %w", err)
	}
	defer func() { _ = buildResult.Body.Close() }()
	if err := drainDockerStream(buildResult.Body); err != nil {
		return Result{}, fmt.Errorf("build failed: %w", err)
	}

	// Anonymous push: the local registry (phase-7-deployment-platform.md
	// Task 2) is unauthenticated, but the Docker Engine API still requires
	// *some* X-Registry-Auth value — base64("{}") is the documented way to
	// authenticate anonymously, the same value the real docker CLI sends
	// when no credentials are configured for a registry host.
	pushResp, err := b.cli.ImagePush(timeoutCtx, tag, client.ImagePushOptions{
		RegistryAuth: base64.StdEncoding.EncodeToString([]byte("{}")),
	})
	if err != nil {
		return Result{}, fmt.Errorf("pushing image: %w", err)
	}
	defer func() { _ = pushResp.Close() }()
	if err := drainDockerStream(pushResp); err != nil {
		return Result{}, fmt.Errorf("push failed: %w", err)
	}

	return Result{CommitSHA: commitSHA, Image: tag}, nil
}

// drainDockerStream reads a Docker Engine API JSON-lines response
// (ImageBuild and ImagePush both use this shape) to completion, surfacing
// the first inline {"error": "..."} object as a real Go error — the HTTP
// status alone is 200 even when a build/push fails partway through, so the
// stream body is the only place the actual failure reason appears (the
// same reason internal/runtime.DockerRuntime.PullImage must fully drain
// its own pull response, just also checking for the error field pull
// doesn't need to).
func drainDockerStream(r io.Reader) error {
	dec := json.NewDecoder(r)
	for {
		var msg struct {
			Error       string `json:"error"`
			ErrorDetail struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := dec.Decode(&msg); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading docker response stream: %w", err)
		}
		if msg.Error != "" {
			if msg.ErrorDetail.Message != "" {
				return fmt.Errorf("%s", msg.ErrorDetail.Message)
			}
			return fmt.Errorf("%s", msg.Error)
		}
	}
}
