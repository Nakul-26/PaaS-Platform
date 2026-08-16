package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"

	"platform/internal/runtime"
)

// fakeRuntime is a hand-rolled ContainerRuntime double — no I/O, so this
// stays a unit test per testing-strategy.md's tier definitions.
type fakeRuntime struct {
	pullErr    error
	createErr  error
	createID   string
	startErr   error
	stopErr    error
	removeErr  error
	statusErr  error
	status     runtime.ContainerStatusInfo
	logs       string
	logsErr    error
	lastSpec   runtime.ContainerSpec
	lastStopTO time.Duration
}

func (f *fakeRuntime) PullImage(ctx context.Context, image string) error { return f.pullErr }

func (f *fakeRuntime) CreateContainer(ctx context.Context, spec runtime.ContainerSpec) (string, error) {
	f.lastSpec = spec
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.createID, nil
}

func (f *fakeRuntime) StartContainer(ctx context.Context, id string) error { return f.startErr }

func (f *fakeRuntime) StopContainer(ctx context.Context, id string, timeout time.Duration) error {
	f.lastStopTO = timeout
	return f.stopErr
}

func (f *fakeRuntime) RemoveContainer(ctx context.Context, id string) error { return f.removeErr }

func (f *fakeRuntime) ContainerStatus(ctx context.Context, id string) (runtime.ContainerStatusInfo, error) {
	if f.statusErr != nil {
		return runtime.ContainerStatusInfo{}, f.statusErr
	}
	return f.status, nil
}

func (f *fakeRuntime) StreamLogs(ctx context.Context, id string, follow bool) (io.ReadCloser, error) {
	if f.logsErr != nil {
		return nil, f.logsErr
	}
	return io.NopCloser(strings.NewReader(f.logs)), nil
}

func newTestServer(rt *fakeRuntime) *Server {
	return New(rt, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHandleCreate(t *testing.T) {
	t.Run("missing image is rejected", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/containers", bytes.NewBufferString(`{}`))
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
		}
	})

	t.Run("invalid JSON is rejected", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/containers", bytes.NewBufferString(`not json`))
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("pull failure surfaces as bad gateway", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{pullErr: errors.New("daemon unreachable")})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/containers", bytes.NewBufferString(`{"image":"nginx:latest"}`))
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
		}
	})

	t.Run("success creates, starts and returns status", func(t *testing.T) {
		rt := &fakeRuntime{
			createID: "abc123",
			status:   runtime.ContainerStatusInfo{ID: "abc123", Status: runtime.StatusRunning},
		}
		s := newTestServer(rt)
		rec := httptest.NewRecorder()
		body := `{"image":"nginx:latest","name":"demo","ports":[{"container_port":80,"protocol":"tcp"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/containers", bytes.NewBufferString(body))
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusCreated, rec.Body.String())
		}
		var resp containerResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
		if resp.ID != "abc123" || resp.Status != string(runtime.StatusRunning) {
			t.Fatalf("unexpected response: %+v", resp)
		}
		if rt.lastSpec.Image != "nginx:latest" || len(rt.lastSpec.Ports) != 1 || rt.lastSpec.Ports[0].ContainerPort != 80 {
			t.Fatalf("spec not passed through correctly: %+v", rt.lastSpec)
		}
	})
}

func TestHandleStatus(t *testing.T) {
	t.Run("unknown container is 404", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{statusErr: errdefs.ErrNotFound.WithMessage("no such container")})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/containers/missing", nil)
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("known container reports status", func(t *testing.T) {
		rt := &fakeRuntime{status: runtime.ContainerStatusInfo{ID: "abc123", Status: runtime.StatusExited, ExitCode: 1}}
		s := newTestServer(rt)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/containers/abc123", nil)
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		var resp containerResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
		if resp.Status != string(runtime.StatusExited) || resp.ExitCode != 1 {
			t.Fatalf("unexpected response: %+v", resp)
		}
	})
}

func TestHandleStop(t *testing.T) {
	t.Run("default timeout applied when body omitted", func(t *testing.T) {
		rt := &fakeRuntime{status: runtime.ContainerStatusInfo{ID: "abc123", Status: runtime.StatusExited}}
		s := newTestServer(rt)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/containers/abc123/stop", nil)
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if rt.lastStopTO != 10*time.Second {
			t.Fatalf("timeout = %v, want 10s default", rt.lastStopTO)
		}
	})

	t.Run("custom timeout honored", func(t *testing.T) {
		rt := &fakeRuntime{status: runtime.ContainerStatusInfo{ID: "abc123", Status: runtime.StatusExited}}
		s := newTestServer(rt)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/containers/abc123/stop", bytes.NewBufferString(`{"timeout_seconds":30}`))
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if rt.lastStopTO != 30*time.Second {
			t.Fatalf("timeout = %v, want 30s", rt.lastStopTO)
		}
	})
}

func TestHandleRemove(t *testing.T) {
	t.Run("success is 204", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/v1/containers/abc123", nil)
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	})

	t.Run("unknown container is 404", func(t *testing.T) {
		s := newTestServer(&fakeRuntime{removeErr: errdefs.ErrNotFound.WithMessage("no such container")})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/v1/containers/missing", nil)
		s.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestHandleLogs(t *testing.T) {
	rt := &fakeRuntime{logs: "line one\nline two\n"}
	s := newTestServer(rt)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/containers/abc123/logs", nil)
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "line one\nline two\n" {
		t.Fatalf("body = %q, want log content", rec.Body.String())
	}
}
