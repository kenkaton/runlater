package cloudtasks

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/kenkaton/runlater"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestDispatchTransportFailureIsAmbiguousAndKeepsIdentity(t *testing.T) {
	transportErr := errors.New("response lost")
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		// Consume the body to model a failure after the request could have reached
		// the provider. At this point the caller cannot know whether Cloud Tasks
		// accepted the task.
		_, _ = io.Copy(io.Discard, r.Body)
		return nil, transportErr
	})}

	d, err := New(Config{
		Project:     "p",
		Location:    "l",
		Queue:       "q",
		TargetURL:   "https://example.com/jobs",
		HTTPClient:  client,
		TokenSource: StaticTokenSource("token"),
		APIEndpoint: "https://cloudtasks.example",
	})
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := d.Dispatch(context.Background(), runlater.Job{
		ID:      "stable-id",
		Name:    "email.send",
		Payload: []byte(`null`),
	})
	if !errors.Is(err, runlater.ErrAmbiguousHandoff) {
		t.Fatalf("err = %v, want ErrAmbiguousHandoff", err)
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("err = %v, want wrapped transport error", err)
	}
	if receipt.ID != "stable-id" {
		t.Fatalf("receipt ID = %q", receipt.ID)
	}
	if !strings.Contains(receipt.ProviderID, "/tasks/") {
		t.Fatalf("ProviderID = %q", receipt.ProviderID)
	}
}
