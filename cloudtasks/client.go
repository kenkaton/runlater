// Package cloudtasks hands runlater jobs to Google Cloud Tasks over its REST
// API, without gRPC, protobuf, or the Google Cloud Go SDK.
//
// Cloud Tasks provides durable, at-least-once delivery. Handlers must be
// idempotent.
package cloudtasks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kenkaton/runlater"
)

const defaultAPIEndpoint = "https://cloudtasks.googleapis.com"

// TokenSource provides OAuth2 access tokens for the Cloud Tasks REST API.
type TokenSource interface {
	Token(context.Context) (string, error)
}

// Config configures the Cloud Tasks dispatcher.
type Config struct {
	Project             string
	Location            string
	Queue               string
	TargetURL           string
	ServiceAccountEmail string
	// Audience sets the OIDC token audience. It requires ServiceAccountEmail:
	// without it no OIDC token is attached and the audience would be ignored.
	Audience    string
	HTTPClient  *http.Client
	TokenSource TokenSource
	APIEndpoint string
}

// APIError reports a non-2xx response from the Cloud Tasks API. Callers can
// use it to tell a handoff worth retrying from one that will never succeed.
type APIError struct {
	StatusCode int
	// Status is the API's canonical error status, such as PERMISSION_DENIED,
	// when the response carried one.
	Status  string
	Message string
	Body    string
}

func (e *APIError) Error() string {
	detail := e.Message
	if detail == "" {
		detail = e.Body
	}
	if e.Status != "" {
		return fmt.Sprintf("cloudtasks: create task: %d %s: %s", e.StatusCode, e.Status, detail)
	}
	return fmt.Sprintf("cloudtasks: create task: %d: %s", e.StatusCode, detail)
}

// Retryable reports whether repeating the handoff could plausibly succeed.
// Because the job carries a stable task name, a retry that arrives after the
// first attempt actually landed is deduplicated rather than duplicated.
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// Dispatcher hands runlater jobs off to Google Cloud Tasks using the REST API.
// Cloud Tasks provides durable, at-least-once delivery. Handlers must be idempotent.
type Dispatcher struct {
	project             string
	location            string
	queue               string
	targetURL           string
	serviceAccountEmail string
	audience            string
	httpClient          *http.Client
	tokenSource         TokenSource
	apiEndpoint         string
}

// New creates a Cloud Tasks dispatcher. If TokenSource is nil, the Google Cloud
// metadata server is used, which is appropriate for Cloud Run and other GCP runtimes.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.Project == "" || cfg.Location == "" || cfg.Queue == "" || cfg.TargetURL == "" {
		return nil, fmt.Errorf("cloudtasks: Project, Location, Queue, and TargetURL are required")
	}
	for _, f := range []struct{ name, value string }{
		{"Project", cfg.Project},
		{"Location", cfg.Location},
		{"Queue", cfg.Queue},
	} {
		if !isResourceID(f.value) {
			return nil, fmt.Errorf("cloudtasks: %s %q contains characters that are not valid in a resource name", f.name, f.value)
		}
	}
	u, err := url.Parse(cfg.TargetURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("cloudtasks: TargetURL must be an absolute http(s) URL")
	}
	if cfg.Audience != "" && cfg.ServiceAccountEmail == "" {
		return nil, fmt.Errorf("cloudtasks: Audience requires ServiceAccountEmail")
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	ts := cfg.TokenSource
	if ts == nil {
		// Deliberately not hc: the metadata server must be reached directly,
		// never through a configured proxy. See NewMetadataTokenSource.
		ts = NewMetadataTokenSource(nil)
	}
	endpoint := strings.TrimRight(cfg.APIEndpoint, "/")
	if endpoint == "" {
		endpoint = defaultAPIEndpoint
	}

	return &Dispatcher{
		project:             cfg.Project,
		location:            cfg.Location,
		queue:               cfg.Queue,
		targetURL:           cfg.TargetURL,
		serviceAccountEmail: cfg.ServiceAccountEmail,
		audience:            cfg.Audience,
		httpClient:          hc,
		tokenSource:         ts,
		apiEndpoint:         endpoint,
	}, nil
}

// isResourceID reports whether s is safe to place in a Cloud Tasks resource
// name unescaped. Everything allowed here is an unreserved URL path character,
// so the same value is also safe in the request path.
func isResourceID(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.', r == ':':
		default:
			return false
		}
	}
	return s != ""
}

type oidcToken struct {
	ServiceAccountEmail string `json:"serviceAccountEmail"`
	Audience            string `json:"audience,omitempty"`
}

type httpRequest struct {
	URL        string            `json:"url"`
	HTTPMethod string            `json:"httpMethod"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       string            `json:"body"`
	OIDCToken  *oidcToken        `json:"oidcToken,omitempty"`
}

type task struct {
	Name         string      `json:"name,omitempty"`
	HTTPRequest  httpRequest `json:"httpRequest"`
	ScheduleTime string      `json:"scheduleTime,omitempty"`
}

type createTaskRequest struct {
	Task task `json:"task"`
}

type createTaskResponse struct {
	Name string `json:"name"`
}

type apiErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

// Dispatch creates one Cloud Task. A stable (Name, ID) pair maps to a stable
// Cloud Tasks task name, making ambiguous client retries safer within Cloud
// Tasks' own task-name deduplication semantics.
//
// When Cloud Tasks reports ALREADY_EXISTS the handoff is treated as successful
// and the receipt is marked Deduplicated. Note that this window outlives
// execution: Cloud Tasks refuses a task name for roughly an hour after the task
// ran or was deleted (about nine days for queues created from queue.yaml), so a
// deduplicated receipt can mean the job already ran. Work that must run again
// needs a different logical ID.
func (d *Dispatcher) Dispatch(ctx context.Context, job runlater.Job) (runlater.Receipt, error) {
	if job.ID == "" {
		return runlater.Receipt{}, runlater.ErrEmptyID
	}

	payload, err := runlater.EncodeEnvelope(job)
	if err != nil {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: encode envelope: %w", err)
	}

	providerID := d.taskName(job.Name, job.ID)
	reqBody := createTaskRequest{Task: task{
		Name: providerID,
		HTTPRequest: httpRequest{
			URL:        d.targetURL,
			HTTPMethod: http.MethodPost,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       base64.StdEncoding.EncodeToString(payload),
		},
	}}
	if d.serviceAccountEmail != "" {
		reqBody.Task.HTTPRequest.OIDCToken = &oidcToken{
			ServiceAccountEmail: d.serviceAccountEmail,
			Audience:            d.audience,
		}
	}
	if !job.RunAt.IsZero() {
		reqBody.Task.ScheduleTime = job.RunAt.UTC().Format(time.RFC3339Nano)
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: marshal request: %w", err)
	}

	token, err := d.tokenSource.Token(ctx)
	if err != nil {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: get access token: %w", err)
	}

	endpoint := fmt.Sprintf("%s/v2/projects/%s/locations/%s/queues/%s/tasks",
		d.apiEndpoint,
		url.PathEscape(d.project),
		url.PathEscape(d.location),
		url.PathEscape(d.queue),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: create task: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var apiErr apiErrorResponse
		_ = json.Unmarshal(b, &apiErr)
		if resp.StatusCode == http.StatusConflict && apiErr.Error.Status == "ALREADY_EXISTS" {
			return runlater.Receipt{ID: job.ID, ProviderID: providerID, Deduplicated: true}, nil
		}
		return runlater.Receipt{}, &APIError{
			StatusCode: resp.StatusCode,
			Status:     apiErr.Error.Status,
			Message:    apiErr.Error.Message,
			Body:       strings.TrimSpace(string(b)),
		}
	}

	// A 2xx means Cloud Tasks accepted responsibility. Response decoding is
	// best-effort: returning an error here would turn a successful handoff into
	// an ambiguous failure and could cause a caller to enqueue a new logical ID.
	var out createTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err == nil && out.Name != "" {
		providerID = out.Name
	}
	// Drain whatever the decoder left behind so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return runlater.Receipt{ID: job.ID, ProviderID: providerID}, nil
}

func (d *Dispatcher) taskName(name, id string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(id))
	taskID := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", d.project, d.location, d.queue, taskID)
}
