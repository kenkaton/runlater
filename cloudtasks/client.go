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
	Audience            string
	HTTPClient          *http.Client
	TokenSource         TokenSource
	APIEndpoint         string
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
	u, err := url.Parse(cfg.TargetURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("cloudtasks: TargetURL must be an absolute http(s) URL")
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
func (d *Dispatcher) Dispatch(ctx context.Context, job runlater.Job) (runlater.Receipt, error) {
	if job.ID == "" {
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: job ID is required")
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
			return runlater.Receipt{ID: job.ID, ProviderID: providerID}, nil
		}
		return runlater.Receipt{}, fmt.Errorf("cloudtasks: create task: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	// A 2xx means Cloud Tasks accepted responsibility. Response decoding is
	// best-effort: returning an error here would turn a successful handoff into
	// an ambiguous failure and could cause a caller to enqueue a new logical ID.
	var out createTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err == nil && out.Name != "" {
		providerID = out.Name
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
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
