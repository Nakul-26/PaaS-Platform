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
	"net/url"
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

// Organization mirrors handlers_organizations.go's organizationResponse
// (phase-6-multi-tenant-saas.md Task 3) — distinct from the Org type above
// (AuthResponse's compact signup/login shape) since this one also carries
// Name/CreatedAt for `platform get org`/`update org` to show.
type Organization struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
}

// GetOrganization calls GET /v1/orgs/:orgId.
func (c *Client) GetOrganization(ctx context.Context, orgID string) (Organization, error) {
	var o Organization
	err := c.do(ctx, http.MethodGet, "/v1/orgs/"+orgID, nil, &o)
	return o, err
}

// UpdateOrganization calls PATCH /v1/orgs/:orgId {name}. There is
// deliberately no DeleteOrganization here yet — Task 3's open decision 2:
// the DELETE route exists and is tested, but a CLI surface for deleting the
// only org a session is authenticated against needs its own
// "what happens next" story first.
func (c *Client) UpdateOrganization(ctx context.Context, orgID, name string) (Organization, error) {
	body, _ := json.Marshal(struct {
		Name string `json:"name"`
	}{name})
	var o Organization
	err := c.do(ctx, http.MethodPatch, "/v1/orgs/"+orgID, body, &o)
	return o, err
}

// Membership mirrors handlers_members.go's membershipResponse
// (phase-6-multi-tenant-saas.md Task 4).
type Membership struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	UserID    string    `json:"user_id"`
	Email     string    `json:"email,omitempty"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

type MembershipPage struct {
	Data []Membership `json:"data"`
}

// ListMembers calls GET /v1/orgs/:orgId/members. What comes back depends on
// the caller's own role — see MembershipRepository.ListByOrg's doc comment
// server-side; an owner/admin session sees the full roster, any other role
// sees only its own membership row.
func (c *Client) ListMembers(ctx context.Context, orgID string) (MembershipPage, error) {
	var page MembershipPage
	err := c.do(ctx, http.MethodGet, "/v1/orgs/"+orgID+"/members", nil, &page)
	return page, err
}

// InviteMember calls POST /v1/orgs/:orgId/members {email, role} — only
// works for an already-registered user (open decision 3).
func (c *Client) InviteMember(ctx context.Context, orgID, email, role string) (Membership, error) {
	body, _ := json.Marshal(struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}{email, role})
	var m Membership
	err := c.do(ctx, http.MethodPost, "/v1/orgs/"+orgID+"/members", body, &m)
	return m, err
}

// ChangeMemberRole calls PATCH /v1/orgs/:orgId/members/:userId {role}.
func (c *Client) ChangeMemberRole(ctx context.Context, orgID, userID, role string) (Membership, error) {
	body, _ := json.Marshal(struct {
		Role string `json:"role"`
	}{role})
	var m Membership
	err := c.do(ctx, http.MethodPatch, "/v1/orgs/"+orgID+"/members/"+userID, body, &m)
	return m, err
}

// RemoveMember calls DELETE /v1/orgs/:orgId/members/:userId.
func (c *Client) RemoveMember(ctx context.Context, orgID, userID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/orgs/"+orgID+"/members/"+userID, nil, nil)
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

// ContainerSummary is one replica of a Deployment — see Deployment.Containers.
type ContainerSummary struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"`
}

// Service is an application's load-balancer routing identity
// (phase-4-service-discovery-lb.md Task 6) — nil until Task 3's controller
// has lazily created one (the application's first-ever healthy instance).
// DNSName is the exact value to send as the load balancer's
// X-Platform-Service routing header (open decision 2) — not a resolvable
// hostname yet, that's Phase 5's job.
type Service struct {
	DNSName string `json:"dns_name"`
}

type Deployment struct {
	ID                string             `json:"id"`
	ApplicationID     string             `json:"application_id"`
	Image             string             `json:"image"`
	Revision          int                `json:"revision"`
	Status            string             `json:"status"`
	Strategy          string             `json:"strategy"`
	WorkerContainerID *string            `json:"worker_container_id,omitempty"`
	ReplicasDesired   int                `json:"replicas_desired"`
	ReplicasRunning   int                `json:"replicas_running"`
	Containers        []ContainerSummary `json:"containers,omitempty"`
	Service           *Service           `json:"service,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	CompletedAt       *time.Time         `json:"completed_at,omitempty"`
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

// ScaleApplication updates appID's desired replica count. It does not wait
// for the change to take effect — the Deployment controller reconciles
// toward it on its own tick (phase-3-controllers.md Task 5).
func (c *Client) ScaleApplication(ctx context.Context, appID string, replicas int) (Application, error) {
	body, _ := json.Marshal(struct {
		ReplicasDesired int `json:"replicas_desired"`
	}{replicas})
	var a Application
	err := c.do(ctx, http.MethodPatch, "/v1/applications/"+appID, body, &a)
	return a, err
}

// Node mirrors handlers_nodes.go's nodeResponse (Task 7).
type Node struct {
	ID              string    `json:"id"`
	Hostname        string    `json:"hostname"`
	Status          string    `json:"status"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}

type NodePage struct {
	Data []Node `json:"data"`
}

// ListNodes calls GET /v1/nodes, gated server-side by requirePlatformAdmin —
// no project or org scoping is needed here since nodes carry no RLS.
func (c *Client) ListNodes(ctx context.Context) (NodePage, error) {
	var page NodePage
	err := c.do(ctx, http.MethodGet, "/v1/nodes", nil, &page)
	return page, err
}

// Domain mirrors handlers_domains.go's domainResponse
// (phase-5-networking-ingress.md Task 2).
type Domain struct {
	ID            string    `json:"id"`
	OrgID         string    `json:"org_id"`
	ProjectID     string    `json:"project_id"`
	ApplicationID string    `json:"application_id"`
	Hostname      string    `json:"hostname"`
	TLSStatus     string    `json:"tls_status"`
	CreatedAt     time.Time `json:"created_at"`
}

type DomainPage struct {
	Data []Domain `json:"data"`
}

// CreateDomain registers hostname under projectID, routing to applicationID
// once the load balancer's Host-header routing picks it up (Task 4).
func (c *Client) CreateDomain(ctx context.Context, projectID, hostname, applicationID string) (Domain, error) {
	body, _ := json.Marshal(struct {
		Hostname      string `json:"hostname"`
		ApplicationID string `json:"application_id"`
	}{hostname, applicationID})
	var d Domain
	err := c.do(ctx, http.MethodPost, "/v1/projects/"+projectID+"/domains", body, &d)
	return d, err
}

func (c *Client) ListDomains(ctx context.Context, projectID string) (DomainPage, error) {
	var page DomainPage
	err := c.do(ctx, http.MethodGet, "/v1/projects/"+projectID+"/domains", nil, &page)
	return page, err
}

func (c *Client) DeleteDomain(ctx context.Context, domainID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/domains/"+domainID, nil, nil)
}

// APIKey mirrors handlers_api_keys.go's apiKeyResponse
// (phase-6-multi-tenant-saas.md Task 5). Key is only ever populated on the
// response to CreateAPIKey — the plaintext is shown exactly once, at
// creation, and never retrievable again.
type APIKey struct {
	ID        string     `json:"id"`
	OrgID     string     `json:"org_id"`
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	CreatedBy string     `json:"created_by"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	Key       string     `json:"key,omitempty"`
}

type APIKeyPage struct {
	Data []APIKey `json:"data"`
}

// CreateAPIKey calls POST /v1/orgs/:orgId/api-keys {name, scopes}.
func (c *Client) CreateAPIKey(ctx context.Context, orgID, name string, scopes []string) (APIKey, error) {
	body, _ := json.Marshal(struct {
		Name   string   `json:"name"`
		Scopes []string `json:"scopes,omitempty"`
	}{name, scopes})
	var k APIKey
	err := c.do(ctx, http.MethodPost, "/v1/orgs/"+orgID+"/api-keys", body, &k)
	return k, err
}

// ListAPIKeys calls GET /v1/orgs/:orgId/api-keys. Only owner/admin sessions
// can call this successfully — api_keys.create gates both routes
// (handleListAPIKeys's own doc comment).
func (c *Client) ListAPIKeys(ctx context.Context, orgID string) (APIKeyPage, error) {
	var page APIKeyPage
	err := c.do(ctx, http.MethodGet, "/v1/orgs/"+orgID+"/api-keys", nil, &page)
	return page, err
}

// DeleteAPIKey calls DELETE /v1/api-keys/:keyId.
func (c *Client) DeleteAPIKey(ctx context.Context, keyID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/api-keys/"+keyID, nil, nil)
}

// Quota mirrors handlers_quota.go's quotaResponse
// (phase-6-multi-tenant-saas.md Task 6): an org's ceilings paired with its
// live usage against each one.
type Quota struct {
	OrgID                string `json:"org_id"`
	MaxCPUMillicores     int    `json:"max_cpu_millicores"`
	MaxMemoryMB          int    `json:"max_memory_mb"`
	MaxContainers        int    `json:"max_containers"`
	MaxProjects          int    `json:"max_projects"`
	MaxDeploymentsPerDay int    `json:"max_deployments_per_day"`
	UsedCPUMillicores    int    `json:"used_cpu_millicores"`
	UsedMemoryMB         int    `json:"used_memory_mb"`
	UsedContainers       int    `json:"used_containers"`
	UsedProjects         int    `json:"used_projects"`
	UsedDeploymentsToday int    `json:"used_deployments_today"`
}

// GetQuota calls GET /v1/orgs/:orgId/quota.
func (c *Client) GetQuota(ctx context.Context, orgID string) (Quota, error) {
	var q Quota
	err := c.do(ctx, http.MethodGet, "/v1/orgs/"+orgID+"/quota", nil, &q)
	return q, err
}

// AuditLogEntry mirrors handlers_audit_logs.go's auditLogResponse
// (phase-6-multi-tenant-saas.md Task 7).
type AuditLogEntry struct {
	ID          string          `json:"id"`
	OrgID       string          `json:"org_id"`
	ActorUserID *string         `json:"actor_user_id,omitempty"`
	Action      string          `json:"action"`
	TargetType  string          `json:"target_type"`
	TargetID    string          `json:"target_id"`
	Metadata    json.RawMessage `json:"metadata"`
	CreatedAt   time.Time       `json:"created_at"`
}

type AuditLogPage struct {
	Data       []AuditLogEntry `json:"data"`
	NextCursor *string         `json:"next_cursor"`
}

// ListAuditLogs calls GET /v1/orgs/:orgId/audit-logs?cursor=, newest first.
// cursor is empty for the first page, or a previous call's NextCursor.
func (c *Client) ListAuditLogs(ctx context.Context, orgID, cursor string) (AuditLogPage, error) {
	path := "/v1/orgs/" + orgID + "/audit-logs"
	if cursor != "" {
		path += "?cursor=" + url.QueryEscape(cursor)
	}
	var page AuditLogPage
	err := c.do(ctx, http.MethodGet, path, nil, &page)
	return page, err
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
