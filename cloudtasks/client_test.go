package cloudtasks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kenkaton/runlater"
)

func TestDispatchCreatesRESTTask(t *testing.T) {
	var got createTaskRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotAuth := r.Header.Get("Authorization"); gotAuth != "Bearer test-token" {
			t.Fatalf("Authorization = %q", gotAuth)
		}
		if r.URL.Path != "/v2/projects/p/locations/asia-northeast1/queues/q/tasks" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"projects/p/locations/asia-northeast1/queues/q/tasks/provider-id"}`))
	}))
	defer server.Close()

	d, err := New(Config{
		Project:             "p",
		Location:            "asia-northeast1",
		Queue:               "q",
		TargetURL:           "https://example.com/internal/jobs",
		ServiceAccountEmail: "tasks@example.iam.gserviceaccount.com",
		Audience:            "https://example.com",
		TokenSource:         StaticTokenSource("test-token"),
		APIEndpoint:         server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}

	runAt := time.Date(2026, 8, 28, 1, 2, 3, 0, time.UTC)
	job := runlater.Job{ID: "welcome-42", Name: "email.send", Payload: json.RawMessage(`{"user_id":42}`), RunAt: runAt}
	receipt, err := d.Dispatch(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != job.ID || !strings.Contains(receipt.ProviderID, "provider-id") {
		t.Fatalf("receipt = %+v", receipt)
	}
	if got.Task.Name == "" {
		t.Fatal("expected deterministic task name")
	}
	if got.Task.ScheduleTime != "2026-08-28T01:02:03Z" {
		t.Fatalf("scheduleTime = %q", got.Task.ScheduleTime)
	}
	if got.Task.HTTPRequest.OIDCToken == nil || got.Task.HTTPRequest.OIDCToken.ServiceAccountEmail == "" {
		t.Fatal("expected OIDC token config")
	}

	decoded, err := base64.StdEncoding.DecodeString(got.Task.HTTPRequest.Body)
	if err != nil {
		t.Fatal(err)
	}
	env, err := runlater.DecodeEnvelope(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != "welcome-42" || env.Name != "email.send" || string(env.Payload) != `{"user_id":42}` {
		t.Fatalf("envelope = %+v", env)
	}
}

func TestTaskNameUsesNameAndID(t *testing.T) {
	d := &Dispatcher{project: "p", location: "l", queue: "q"}

	a := d.taskName("email.send", "42")
	if a != d.taskName("email.send", "42") {
		t.Fatal("same name and ID must produce the same task name")
	}
	if a == d.taskName("audit.write", "42") {
		t.Fatal("different job names with the same logical ID must not collide")
	}
	if a == d.taskName("email.send", "43") {
		t.Fatal("different logical IDs must not collide")
	}
}

func TestDispatchTreatsAlreadyExistsAsIdempotentSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":409,"message":"task exists","status":"ALREADY_EXISTS"}}`))
	}))
	defer server.Close()

	d, err := New(Config{
		Project:     "p",
		Location:    "l",
		Queue:       "q",
		TargetURL:   "https://example.com/jobs",
		TokenSource: StaticTokenSource("test-token"),
		APIEndpoint: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := d.Dispatch(context.Background(), runlater.Job{ID: "same", Name: "x", Payload: json.RawMessage(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != "same" || receipt.ProviderID == "" {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestDispatchDoesNotTreatArbitraryConflictAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "conflict", http.StatusConflict)
	}))
	defer server.Close()
	d, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("x"), APIEndpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "id", Name: "x", Payload: json.RawMessage(`null`)}); err == nil {
		t.Fatal("expected error")
	}
}

func TestDispatchReturnsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "permission denied", http.StatusForbidden)
	}))
	defer server.Close()

	d, err := New(Config{
		Project:     "p",
		Location:    "l",
		Queue:       "q",
		TargetURL:   "https://example.com/jobs",
		TokenSource: StaticTokenSource("test-token"),
		APIEndpoint: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "id", Name: "x", Payload: json.RawMessage(`null`)}); err == nil {
		t.Fatal("expected error")
	}
}

func TestNewRejectsRelativeTarget(t *testing.T) {
	_, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "/jobs", TokenSource: StaticTokenSource("x")})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ALREADY_EXISTS means Cloud Tasks refused the task name, which it also does
// for roughly an hour after the task ran. Callers must be able to tell that
// apart from a fresh handoff.
func TestDispatchMarksDeduplicatedReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":409,"message":"task exists","status":"ALREADY_EXISTS"}}`))
	}))
	defer server.Close()

	d, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("t"), APIEndpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := d.Dispatch(context.Background(), runlater.Job{ID: "same", Name: "x", Payload: json.RawMessage(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Deduplicated {
		t.Fatalf("receipt = %+v, want Deduplicated", receipt)
	}
}

func TestDispatchSuccessIsNotDeduplicated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"name":"projects/p/locations/l/queues/q/tasks/abc"}`))
	}))
	defer server.Close()

	d, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("t"), APIEndpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := d.Dispatch(context.Background(), runlater.Job{ID: "id", Name: "x", Payload: json.RawMessage(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Deduplicated {
		t.Fatalf("receipt = %+v, want fresh handoff", receipt)
	}
}

func TestDispatchAPIErrorClassifiesRetryability(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantRetryable bool
		wantStatus    string
	}{
		{"permission denied", http.StatusForbidden, `{"error":{"code":403,"message":"denied","status":"PERMISSION_DENIED"}}`, false, "PERMISSION_DENIED"},
		{"rate limited", http.StatusTooManyRequests, `{"error":{"code":429,"message":"slow down","status":"RESOURCE_EXHAUSTED"}}`, true, "RESOURCE_EXHAUSTED"},
		{"unavailable", http.StatusServiceUnavailable, `{"error":{"code":503,"message":"try later","status":"UNAVAILABLE"}}`, true, "UNAVAILABLE"},
		{"non-json body", http.StatusBadRequest, "plain text failure", false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			d, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("t"), APIEndpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, err = d.Dispatch(context.Background(), runlater.Job{ID: "id", Name: "x", Payload: json.RawMessage(`null`)})
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err = %v, want *APIError", err)
			}
			if apiErr.StatusCode != tt.status || apiErr.Status != tt.wantStatus {
				t.Fatalf("apiErr = %+v", apiErr)
			}
			if apiErr.Retryable() != tt.wantRetryable {
				t.Fatalf("Retryable() = %v, want %v", apiErr.Retryable(), tt.wantRetryable)
			}
		})
	}
}

func TestNewValidatesConfig(t *testing.T) {
	base := Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("t")}
	mutate := func(f func(*Config)) Config {
		c := base
		f(&c)
		return c
	}
	tests := []struct {
		name string
		cfg  Config
	}{
		{"project with slash", mutate(func(c *Config) { c.Project = "p/../other" })},
		{"location with space", mutate(func(c *Config) { c.Location = "asia northeast1" })},
		{"queue with percent", mutate(func(c *Config) { c.Queue = "q%2f" })},
		{"audience without service account", mutate(func(c *Config) { c.Audience = "https://example.com" })},
		{"relative target", mutate(func(c *Config) { c.TargetURL = "/jobs" })},
		{"missing queue", mutate(func(c *Config) { c.Queue = "" })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(base); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestDispatchRequiresJobID(t *testing.T) {
	d, err := New(Config{Project: "p", Location: "l", Queue: "q", TargetURL: "https://example.com/jobs", TokenSource: StaticTokenSource("t")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Dispatch(context.Background(), runlater.Job{Name: "x"}); !errors.Is(err, runlater.ErrEmptyID) {
		t.Fatalf("err = %v, want ErrEmptyID", err)
	}
}
