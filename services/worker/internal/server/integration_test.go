//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"platform/internal/runtime"
)

// TestWorkerHTTP_ContainerLifecycle is the Task 4 acceptance test: the
// worker agent, driven only over its HTTP contract (never through the
// runtime.ContainerRuntime interface directly), starts, inspects, reads
// logs from, stops and removes a real container against a real Docker
// daemon.
func TestWorkerHTTP_ContainerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Fatalf("connecting to docker daemon: %v", err)
	}
	defer func() { _ = rt.Close() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(New(rt, logger).Routes())
	defer ts.Close()

	createBody := `{"image":"nginx:latest","name":"platform-worker-http-test-nginx","ports":[{"container_port":80,"protocol":"tcp"}]}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/containers", bytes.NewBufferString(createBody))
	if err != nil {
		t.Fatalf("building create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/containers: %v", err)
	}
	var created containerResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decoding create response: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d (id=%q status=%q)", resp.StatusCode, http.StatusCreated, created.ID, created.Status)
	}
	id := created.ID

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		delReq, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, ts.URL+"/v1/containers/"+id, nil)
		if resp, err := http.DefaultClient.Do(delReq); err == nil {
			_ = resp.Body.Close()
		}
	})

	// Poll until the worker reports the container running.
	deadline := time.Now().Add(15 * time.Second)
	var status containerResponse
	for time.Now().Before(deadline) {
		status = getStatus(t, ctx, ts.URL, id)
		if status.Status == string(runtime.StatusRunning) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if status.Status != string(runtime.StatusRunning) {
		t.Fatalf("container did not reach running status in time, last status=%q", status.Status)
	}

	logsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/containers/"+id+"/logs?follow=false", nil)
	if err != nil {
		t.Fatalf("building logs request: %v", err)
	}
	logsResp, err := http.DefaultClient.Do(logsReq)
	if err != nil {
		t.Fatalf("GET logs: %v", err)
	}
	logsBody, err := io.ReadAll(logsResp.Body)
	_ = logsResp.Body.Close()
	if err != nil {
		t.Fatalf("reading logs body: %v", err)
	}
	if logsResp.StatusCode != http.StatusOK {
		t.Fatalf("logs status = %d, want %d", logsResp.StatusCode, http.StatusOK)
	}
	if len(logsBody) == 0 {
		t.Fatal("expected at least some log output from nginx, got none")
	}

	stopReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/containers/"+id+"/stop", bytes.NewBufferString(`{"timeout_seconds":5}`))
	if err != nil {
		t.Fatalf("building stop request: %v", err)
	}
	stopResp, err := http.DefaultClient.Do(stopReq)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	var stopped containerResponse
	if err := json.NewDecoder(stopResp.Body).Decode(&stopped); err != nil {
		t.Fatalf("decoding stop response: %v", err)
	}
	_ = stopResp.Body.Close()
	if stopResp.StatusCode != http.StatusOK || stopped.Status != string(runtime.StatusExited) {
		t.Fatalf("stop response = %+v, status=%d, want exited/200", stopped, stopResp.StatusCode)
	}

	delReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, ts.URL+"/v1/containers/"+id, nil)
	if err != nil {
		t.Fatalf("building delete request: %v", err)
	}
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d", delResp.StatusCode, http.StatusNoContent)
	}

	finalReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/containers/"+id, nil)
	if err != nil {
		t.Fatalf("building final status request: %v", err)
	}
	finalResp, err := http.DefaultClient.Do(finalReq)
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	_ = finalResp.Body.Close()
	if finalResp.StatusCode != http.StatusNotFound {
		t.Fatalf("status after delete = %d, want %d", finalResp.StatusCode, http.StatusNotFound)
	}
}

func getStatus(t *testing.T, ctx context.Context, baseURL, id string) containerResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/containers/"+id, nil)
	if err != nil {
		t.Fatalf("building status request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out containerResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding status response: %v", err)
	}
	return out
}
