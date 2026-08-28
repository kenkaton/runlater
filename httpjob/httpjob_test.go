package httpjob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kenkaton/runlater"
)

func TestHandleJSON(t *testing.T) {
	type payload struct {
		UserID int `json:"user_id"`
	}

	mux := New()
	var got payload
	if err := HandleJSON(mux, "email.send", func(_ context.Context, p payload) error {
		got = p
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	body, err := runlater.EncodeEnvelope(runlater.Job{ID: "id-1", Name: "email.send", Payload: []byte(`{"user_id":42}`)})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if got.UserID != 42 {
		t.Fatalf("payload = %+v", got)
	}
}

func TestHandlerErrorIsRetryableStatus(t *testing.T) {
	mux := New()
	if err := mux.Handle("x", func(context.Context, runlater.Envelope) error {
		return context.DeadlineExceeded
	}); err != nil {
		t.Fatal(err)
	}
	body, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id", Name: "x", Payload: []byte(`null`)})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestUnknownProtocolVersionIsBadRequest(t *testing.T) {
	mux := New()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"version":99,"id":"id","name":"x","payload":null}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", w.Code)
	}
}

// A payload that cannot decode will never decode, so retrying it only burns
// the queue's attempt budget. The mux must report success to stop redelivery
// while still surfacing the failure to the operator.
func TestUndecodablePayloadIsNotRetried(t *testing.T) {
	type payload struct {
		UserID int `json:"user_id"`
	}
	mux := New()
	var gotErr error
	mux.ErrorHandler = func(_ *http.Request, _ runlater.Envelope, err error) { gotErr = err }
	if err := HandleJSON(mux, "email.send", func(context.Context, payload) error {
		t.Fatal("handler must not run")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	body, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id", Name: "email.send", Payload: []byte(`"not-an-object"`)})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if !errors.Is(gotErr, ErrPermanent) {
		t.Fatalf("reported err = %v, want ErrPermanent", gotErr)
	}
}

func TestPermanentHandlerErrorStopsRetries(t *testing.T) {
	mux := New()
	if err := mux.Handle("x", func(context.Context, runlater.Envelope) error {
		return fmt.Errorf("bad input: %w", ErrPermanent)
	}); err != nil {
		t.Fatal(err)
	}
	body, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id", Name: "x", Payload: []byte(`null`)})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestErrorHandlerSeesHandlerFailure(t *testing.T) {
	mux := New()
	var gotEnv runlater.Envelope
	var gotErr error
	mux.ErrorHandler = func(_ *http.Request, env runlater.Envelope, err error) { gotEnv, gotErr = env, err }
	want := errors.New("boom")
	if err := mux.Handle("x", func(context.Context, runlater.Envelope) error { return want }); err != nil {
		t.Fatal(err)
	}
	body, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id-9", Name: "x", Payload: []byte(`null`)})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", w.Code)
	}
	if !errors.Is(gotErr, want) || gotEnv.ID != "id-9" {
		t.Fatalf("reported env = %+v, err = %v", gotEnv, gotErr)
	}
}

func TestRequestRejections(t *testing.T) {
	mux := New()
	if err := mux.Handle("known", func(context.Context, runlater.Envelope) error { return nil }); err != nil {
		t.Fatal(err)
	}
	known, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id", Name: "known", Payload: []byte(`null`)})
	unknown, _ := runlater.EncodeEnvelope(runlater.Job{ID: "id", Name: "missing", Payload: []byte(`null`)})
	oversized, _ := runlater.EncodeEnvelope(runlater.Job{
		ID:      "id",
		Name:    "known",
		Payload: append(append([]byte(`"`), bytes.Repeat([]byte("x"), maxEnvelopeBytes)...), '"'),
	})

	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{"get", http.MethodGet, nil, http.StatusMethodNotAllowed},
		{"empty body", http.MethodPost, nil, http.StatusBadRequest},
		{"unknown job", http.MethodPost, unknown, http.StatusNotFound},
		{"oversized", http.MethodPost, oversized, http.StatusRequestEntityTooLarge},
		{"known job", http.MethodPost, known, http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reported int
			mux.ErrorHandler = func(*http.Request, runlater.Envelope, error) { reported++ }
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(tt.method, "/", bytes.NewReader(tt.body)))
			if w.Code != tt.want {
				t.Fatalf("status = %d, want %d", w.Code, tt.want)
			}
			wantReported := 1
			if tt.want == http.StatusNoContent {
				wantReported = 0
			}
			if reported != wantReported {
				t.Fatalf("ErrorHandler calls = %d, want %d", reported, wantReported)
			}
		})
	}
}

func TestMethodNotAllowedAdvertisesPost(t *testing.T) {
	w := httptest.NewRecorder()
	New().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := w.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow = %q", got)
	}
}
