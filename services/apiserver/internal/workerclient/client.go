// Package workerclient is the API server's client for the worker agent's
// HTTP contract (docs/worker-agent-contract.md, ADR-0012 — this is the only
// way apiserver may reach worker, never by importing its package tree).
// Phase 1 has exactly one worker (phase-1-mvp.md Task 5); the base URL is
// plain config, not service discovery, until Phase 2 introduces real
// scheduling across multiple workers.
package workerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrNotFound is returned when the worker reports 404 container_not_found
// (docs/worker-agent-contract.md). Callers use this to tell "already gone"
// apart from a genuine failure — e.g. deleting an application whose
// container the worker already reaped.
var ErrNotFound = errors.New("workerclient: container not found")

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

type PortBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

type CreateContainerRequest struct {
	Image string            `json:"image"`
	Name  string            `json:"name"`
	Env   map[string]string `json:"env,omitempty"`
	Ports []PortBinding     `json:"ports,omitempty"`
}

// ContainerStatus mirrors the worker's status response
// (docs/worker-agent-contract.md).
type ContainerStatus struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	ExitCode  int       `json:"exit_code,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
}

func (c *Client) CreateContainer(ctx context.Context, req CreateContainerRequest) (ContainerStatus, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return ContainerStatus{}, fmt.Errorf("marshaling create-container request: %w", err)
	}
	var status ContainerStatus
	err = c.do(ctx, http.MethodPost, "/v1/containers", bytes.NewReader(body), http.StatusCreated, &status)
	return status, err
}

func (c *Client) ContainerStatus(ctx context.Context, id string) (ContainerStatus, error) {
	var status ContainerStatus
	err := c.do(ctx, http.MethodGet, "/v1/containers/"+id, nil, http.StatusOK, &status)
	return status, err
}

func (c *Client) StopContainer(ctx context.Context, id string, timeoutSeconds int) (ContainerStatus, error) {
	body, err := json.Marshal(struct {
		TimeoutSeconds int `json:"timeout_seconds"`
	}{TimeoutSeconds: timeoutSeconds})
	if err != nil {
		return ContainerStatus{}, fmt.Errorf("marshaling stop request: %w", err)
	}
	var status ContainerStatus
	err = c.do(ctx, http.MethodPost, "/v1/containers/"+id+"/stop", bytes.NewReader(body), http.StatusOK, &status)
	return status, err
}

func (c *Client) RemoveContainer(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/containers/"+id, nil, http.StatusNoContent, nil)
}

type workerErrorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *Client) do(ctx context.Context, method, path string, body *bytes.Reader, wantStatus int, out any) error {
	var reqBody io.Reader = http.NoBody
	if body != nil {
		reqBody = body
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("building worker request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling worker %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != wantStatus {
		var envelope workerErrorEnvelope
		_ = json.NewDecoder(resp.Body).Decode(&envelope)
		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if envelope.Error.Message != "" {
			return fmt.Errorf("worker %s %s: %s (%s)", method, path, envelope.Error.Message, envelope.Error.Code)
		}
		return fmt.Errorf("worker %s %s: unexpected status %d", method, path, resp.StatusCode)
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding worker response for %s %s: %w", method, path, err)
	}
	return nil
}
