package httpjob

import (
	"bytes"
	"context"
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
