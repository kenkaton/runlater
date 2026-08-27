package cloudtasks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"tasks/1"}`))
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
	job := runlater.Job{Name: "email.send", Payload: json.RawMessage(`{"user_id":42}`), RunAt: runAt}
	if err := d.Dispatch(context.Background(), job); err != nil {
		t.Fatal(err)
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
	var env envelope
	if err := json.Unmarshal(decoded, &env); err != nil {
		t.Fatal(err)
	}
	if env.Name != "email.send" || string(env.Payload) != `{"user_id":42}` {
		t.Fatalf("envelope = %+v", env)
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
	if err := d.Dispatch(context.Background(), runlater.Job{Name: "x", Payload: json.RawMessage(`null`)}); err == nil {
		t.Fatal("expected error")
	}
}
