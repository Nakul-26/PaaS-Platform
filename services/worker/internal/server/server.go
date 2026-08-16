// Package server implements the worker agent's internal HTTP contract
// (docs/worker-agent-contract.md, Phase 1 Task 4) on top of the
// runtime.ContainerRuntime interface (ADR-0006). It is not the platform's
// public API (see api-conventions.md for that) — only the API server calls
// this contract, over the network, per ADR-0012.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/containerd/errdefs"

	"platform/internal/runtime"
)

// Server exposes the worker's container-lifecycle operations over HTTP.
type Server struct {
	runtime runtime.ContainerRuntime
	logger  *slog.Logger
}

func New(rt runtime.ContainerRuntime, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{runtime: rt, logger: logger}
}

// Routes returns the worker's HTTP handler, ready to pass to http.Server.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/containers", s.handleCreate)
	mux.HandleFunc("GET /v1/containers/{id}", s.handleStatus)
	mux.HandleFunc("POST /v1/containers/{id}/stop", s.handleStop)
	mux.HandleFunc("DELETE /v1/containers/{id}", s.handleRemove)
	mux.HandleFunc("GET /v1/containers/{id}/logs", s.handleLogs)
	return mux
}

type createContainerRequest struct {
	Image   string            `json:"image"`
	Name    string            `json:"name"`
	Env     map[string]string `json:"env,omitempty"`
	Ports   []portBinding     `json:"ports,omitempty"`
	Command []string          `json:"command,omitempty"`
}

type portBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

type stopRequest struct {
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

type containerResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	ExitCode  int       `json:"exit_code,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
}

func toContainerResponse(info runtime.ContainerStatusInfo) containerResponse {
	return containerResponse{
		ID:        info.ID,
		Status:    string(info.Status),
		ExitCode:  info.ExitCode,
		StartedAt: info.StartedAt,
	}
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createContainerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body must be valid JSON matching the create-container schema")
		return
	}
	if req.Image == "" {
		writeError(w, http.StatusUnprocessableEntity, "missing_field", "image is required")
		return
	}

	ctx := r.Context()
	if err := s.runtime.PullImage(ctx, req.Image); err != nil {
		s.logger.Error("pulling image", "image", req.Image, "error", err)
		writeError(w, http.StatusBadGateway, "pull_failed", fmt.Sprintf("pulling image %s: %v", req.Image, err))
		return
	}

	spec := runtime.ContainerSpec{
		Image:   req.Image,
		Name:    req.Name,
		Env:     req.Env,
		Command: req.Command,
	}
	for _, p := range req.Ports {
		spec.Ports = append(spec.Ports, runtime.PortBinding{
			ContainerPort: p.ContainerPort,
			HostPort:      p.HostPort,
			Protocol:      p.Protocol,
		})
	}

	id, err := s.runtime.CreateContainer(ctx, spec)
	if err != nil {
		s.logger.Error("creating container", "image", req.Image, "error", err)
		writeError(w, http.StatusBadGateway, "create_failed", fmt.Sprintf("creating container: %v", err))
		return
	}

	if err := s.runtime.StartContainer(ctx, id); err != nil {
		s.logger.Error("starting container", "container_id", id, "error", err)
		writeError(w, http.StatusBadGateway, "start_failed", fmt.Sprintf("starting container %s: %v", id, err))
		return
	}

	info, err := s.runtime.ContainerStatus(ctx, id)
	if err != nil {
		s.logger.Error("inspecting container after start", "container_id", id, "error", err)
		writeError(w, http.StatusBadGateway, "status_failed", fmt.Sprintf("inspecting container %s: %v", id, err))
		return
	}

	s.logger.Info("container started", "container_id", id, "image", req.Image)
	writeJSON(w, http.StatusCreated, toContainerResponse(info))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	info, err := s.runtime.ContainerStatus(r.Context(), id)
	if err != nil {
		s.writeRuntimeError(w, "inspecting", id, err)
		return
	}
	writeJSON(w, http.StatusOK, toContainerResponse(info))
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	timeout := 10 * time.Second
	if r.ContentLength != 0 {
		var req stopRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", "request body must be valid JSON matching the stop schema")
			return
		}
		if req.TimeoutSeconds > 0 {
			timeout = time.Duration(req.TimeoutSeconds) * time.Second
		}
	}

	if err := s.runtime.StopContainer(r.Context(), id, timeout); err != nil {
		s.writeRuntimeError(w, "stopping", id, err)
		return
	}

	info, err := s.runtime.ContainerStatus(r.Context(), id)
	if err != nil {
		s.writeRuntimeError(w, "inspecting", id, err)
		return
	}
	writeJSON(w, http.StatusOK, toContainerResponse(info))
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.runtime.RemoveContainer(r.Context(), id); err != nil {
		s.writeRuntimeError(w, "removing", id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	follow := r.URL.Query().Get("follow") == "true"

	logs, err := s.runtime.StreamLogs(r.Context(), id, follow)
	if err != nil {
		s.writeRuntimeError(w, "streaming logs for", id, err)
		return
	}
	defer func() { _ = logs.Close() }()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	buf := make([]byte, 4096)
	for {
		n, readErr := logs.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				s.logger.Error("streaming logs", "container_id", id, "error", readErr)
			}
			return
		}
	}
}

// writeRuntimeError maps a ContainerRuntime error to an HTTP response,
// distinguishing "no such container" (404) from any other runtime failure
// (502, since the failure is in the worker's upstream, the container
// engine) via errdefs — the classification moby/moby/client itself uses.
func (s *Server) writeRuntimeError(w http.ResponseWriter, action, id string, err error) {
	s.logger.Error(action+" container", "container_id", id, "error", err)
	if errdefs.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "container_not_found", fmt.Sprintf("no container with id %s", id))
		return
	}
	writeError(w, http.StatusBadGateway, "runtime_error", fmt.Sprintf("%s container %s: %v", action, id, err))
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
