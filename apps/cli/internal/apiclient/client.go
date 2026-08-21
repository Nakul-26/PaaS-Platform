// Package apiclient is the CLI's thin REST client for the API server
// (docs/api-conventions.md, ADR-0007) — the CLI is a plain client of the
// same public API the dashboard uses, so this package does nothing beyond
// shaping requests/responses; every actual check happens server-side.
package apiclient

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

// ErrUnauthorized is returned when the server rejects a request with 401
// even after a token-refresh attempt — callers use this to prompt
// re-login.
var ErrUnauthorized = errors.New("apiclient: not authenticated")

// TokenStore lets the client read the current tokens, persist rotated ones
// after a transparent refresh, and hand back the moment before that so the
// call site's own state changes can be saved alongside it.
type TokenStore interface {
	GetAccessToken() string
	GetRefreshToken() string
	SetTokens(accessToken, refreshToken string)
	Save() error
}

// requestTimeout bounds non-streaming calls only — it's applied per-call via
// context, not as the http.Client's blanket Timeout, because that Timeout
// covers the entire response body read and would silently cut off a
// `platform logs --follow` stream after 30s (net/http docs: Client.Timeout
// "includes ... reading the response body").
const requestTimeout = 30 * time.Second

type Client struct {
	baseURL string
	http    *http.Client
	tokens  TokenStore
}

func New(baseURL string, tokens TokenStore) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{},
		tokens:  tokens,
	}
}

type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// APIError is a server-reported error (api-conventions.md §4) — Message is
// documented as safe to show directly to the user.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

type Org struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Role string `json:"role"`
}

type AuthResponse struct {
	User struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"user"`
	Org                  Org       `json:"org"`
	AccessToken          string    `json:"access_token"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
	RefreshToken         string    `json:"refresh_token"`
}

func (c *Client) Signup(ctx context.Context, email, password string) (AuthResponse, error) {
	return c.auth(ctx, "/v1/auth/signup", email, password)
}

func (c *Client) Login(ctx context.Context, email, password string) (AuthResponse, error) {
	return c.auth(ctx, "/v1/auth/login", email, password)
}

func (c *Client) auth(ctx context.Context, path, email, password string) (AuthResponse, error) {
	// #nosec G117 -- request body for POST /v1/auth/{login,signup}; the
	// password is user-supplied input being sent to its own auth endpoint,
	// not a hardcoded credential.
	body, _ := json.Marshal(struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}{email, password})

	var resp AuthResponse
	err := c.raw(ctx, http.MethodPost, path, body, false, &resp)
	return resp, err
}

type Project struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
}

func (c *Client) CreateProject(ctx context.Context, orgID, name string) (Project, error) {
	body, _ := json.Marshal(struct {
		Name string `json:"name"`
	}{name})
	var p Project
	err := c.do(ctx, http.MethodPost, "/v1/orgs/"+orgID+"/projects", body, &p)
	return p, err
}

type Application struct {
	ID              string    `json:"id"`
	OrgID           string    `json:"org_id"`
	ProjectID       string    `json:"project_id"`
	Name            string    `json:"name"`
	Image           string    `json:"image"`
	ReplicasDesired int       `json:"replicas_desired"`
	CreatedAt       time.Time `json:"created_at"`
}

// PortSpec is a container-port -> host-port binding request
// (api-conventions.md's application create body). HostPort of 0 lets the
// container engine pick an ephemeral port.
type PortSpec struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

func (c *Client) CreateApplication(ctx context.Context, projectID, name, image string, ports []PortSpec) (Application, error) {
	body, _ := json.Marshal(struct {
		Name  string     `json:"name"`
		Image string     `json:"image"`
		Ports []PortSpec `json:"ports,omitempty"`
	}{name, image, ports})
	var a Application
	err := c.do(ctx, http.MethodPost, "/v1/projects/"+projectID+"/applications", body, &a)
	return a, err
}

type Deployment struct {
	ID                string     `json:"id"`
	ApplicationID     string     `json:"application_id"`
	Image             string     `json:"image"`
	Revision          int        `json:"revision"`
	Status            string     `json:"status"`
	Strategy          string     `json:"strategy"`
	WorkerContainerID *string    `json:"worker_container_id,omitempty"`
	NodeID            *string    `json:"node_id,omitempty"`
	ContainerStatus   *string    `json:"container_status,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

// Deploy triggers a deployment for appID. image may be empty to redeploy
// the application's current image unchanged (handleDeploy's documented
// behavior).
func (c *Client) Deploy(ctx context.Context, appID, image string) (Deployment, error) {
	var body []byte
	if image != "" {
		body, _ = json.Marshal(struct {
			Image string `json:"image"`
		}{image})
	}
	var d Deployment
	err := c.do(ctx, http.MethodPost, "/v1/applications/"+appID+"/deployments", body, &d)
	return d, err
}

type DeploymentPage struct {
	Data       []Deployment `json:"data"`
	NextCursor *string      `json:"next_cursor"`
}

func (c *Client) ListDeployments(ctx context.Context, appID string) (DeploymentPage, error) {
	var page DeploymentPage
	err := c.do(ctx, http.MethodGet, "/v1/applications/"+appID+"/deployments", nil, &page)
	return page, err
}

func (c *Client) DeleteApplication(ctx context.Context, appID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/applications/"+appID, nil, nil)
}

// StreamLogs proxies GET /v1/applications/:appId/logs (Task 7) and copies
// the response body verbatim to w as it arrives — the CLI does no framing
// of its own, it just surfaces whatever the server sends.
func (c *Client) StreamLogs(ctx context.Context, appID string, follow bool, w io.Writer) error {
	path := "/v1/applications/" + appID + "/logs"
	if follow {
		path += "?follow=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.tokens.GetAccessToken())

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling apiserver GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return decodeAPIError(resp)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// do performs an authenticated request, transparently retrying once via a
// refresh-token exchange on a 401.
func (c *Client) do(ctx context.Context, method, path string, body []byte, out any) error {
	err := c.raw(ctx, method, path, body, true, out)
	if !errors.Is(err, ErrUnauthorized) || c.tokens.GetRefreshToken() == "" {
		return err
	}

	// #nosec G117 -- request body for POST /v1/auth/refresh; this is the
	// caller's own already-issued refresh token being presented back to the
	// server that issued it, not a hardcoded credential.
	refreshBody, _ := json.Marshal(struct {
		RefreshToken string `json:"refresh_token"`
	}{c.tokens.GetRefreshToken()})
	var refreshed struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if refreshErr := c.raw(ctx, http.MethodPost, "/v1/auth/refresh", refreshBody, false, &refreshed); refreshErr != nil {
		return err // surface the original 401, refresh didn't help
	}
	c.tokens.SetTokens(refreshed.AccessToken, refreshed.RefreshToken)
	_ = c.tokens.Save()

	return c.raw(ctx, method, path, body, true, out)
}

func (c *Client) raw(ctx context.Context, method, path string, body []byte, authenticate bool, out any) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	var reqBody io.Reader = http.NoBody
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authenticate {
		req.Header.Set("Authorization", "Bearer "+c.tokens.GetAccessToken())
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling apiserver %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		apiErr := decodeAPIError(resp)
		if resp.StatusCode == http.StatusUnauthorized {
			return errors.Join(ErrUnauthorized, apiErr)
		}
		return apiErr
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response for %s %s: %w", method, path, err)
	}
	return nil
}

func decodeAPIError(resp *http.Response) *APIError {
	var envelope errorEnvelope
	_ = json.NewDecoder(resp.Body).Decode(&envelope)
	if envelope.Error.Message == "" {
		envelope.Error.Code = "unexpected_status"
		envelope.Error.Message = fmt.Sprintf("unexpected status %d", resp.StatusCode)
	}
	return &APIError{Status: resp.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message}
}
