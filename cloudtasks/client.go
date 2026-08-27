package cloudtasks

import (
	"bytes"
	"context"
	"encoding/base64"
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
	Audience            string
	HTTPClient          *http.Client
	TokenSource         TokenSource
	APIEndpoint         string
}

// Dispatcher persists runlater jobs in Google Cloud Tasks using the REST API.
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
	if _, err := url.ParseRequestURI(cfg.TargetURL); err != nil {
		return nil, fmt.Errorf("cloudtasks: invalid TargetURL: %w", err)
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	ts := cfg.TokenSource
	if ts == nil {
		ts = NewMetadataTokenSource(hc)
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

type envelope struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
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
	HTTPRequest  httpRequest `json:"httpRequest"`
	ScheduleTime string      `json:"scheduleTime,omitempty"`
}

type createTaskRequest struct {
	Task task `json:"task"`
}

// Dispatch creates one Cloud Task. The target receives a JSON envelope with
// "name" and "payload" fields.
func (d *Dispatcher) Dispatch(ctx context.Context, job runlater.Job) error {
	payload, err := json.Marshal(envelope{Name: job.Name, Payload: job.Payload})
	if err != nil {
		return fmt.Errorf("cloudtasks: marshal envelope: %w", err)
	}

	reqBody := createTaskRequest{Task: task{HTTPRequest: httpRequest{
		URL:        d.targetURL,
		HTTPMethod: http.MethodPost,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       base64.StdEncoding.EncodeToString(payload),
	}}}
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
		return fmt.Errorf("cloudtasks: marshal request: %w", err)
	}

	token, err := d.tokenSource.Token(ctx)
	if err != nil {
		return fmt.Errorf("cloudtasks: get access token: %w", err)
	}

	endpoint := fmt.Sprintf("%s/v2/projects/%s/locations/%s/queues/%s/tasks",
		d.apiEndpoint,
		url.PathEscape(d.project),
		url.PathEscape(d.location),
		url.PathEscape(d.queue),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cloudtasks: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cloudtasks: create task: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("cloudtasks: create task: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}
