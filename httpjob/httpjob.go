// Package httpjob adapts the runlater wire protocol to net/http handlers.
//
// The mux performs no authentication. Anything it can reach can enqueue work
// into the application, so the endpoint must be protected by the deployment:
// an internal-only route, Cloud Run IAM plus task-level OIDC, a gateway, or
// equivalent. Keeping IAM at the edge is deliberate, but it is only safe if
// the edge actually exists.
package httpjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/kenkaton/runlater"
)

const maxEnvelopeBytes = 1 << 20

// ErrPermanent marks a job failure that retrying cannot fix, such as a payload
// that will never decode. A handler error wrapping it makes ServeHTTP answer
// 200 so the provider stops redelivering the job.
//
// 200 is the only way to stop a push-style provider: Cloud Tasks retries every
// non-2xx response, 4xx included. The failure is still reported to ErrorHandler,
// so "not retried" does not mean "not observed".
var ErrPermanent = errors.New("httpjob: permanent job failure")

// Handler processes one decoded runlater envelope.
type Handler func(context.Context, runlater.Envelope) error

// ErrorHandler observes a rejected or failed job. env is the zero Envelope when
// the request was rejected before decoding succeeded.
type ErrorHandler func(r *http.Request, env runlater.Envelope, err error)

// Mux routes runlater envelopes by job name. It is safe for concurrent use.
type Mux struct {
	// ErrorHandler, if set, is called for every request the mux rejects and
	// every error a handler returns. Without it those errors are unobservable:
	// the HTTP response carries no detail, by design. Set it before serving.
	ErrorHandler ErrorHandler

	mu       sync.RWMutex
	handlers map[string]Handler
}

// New creates an empty job mux.
func New() *Mux {
	return &Mux{handlers: make(map[string]Handler)}
}

// Handle registers a low-level envelope handler.
func (m *Mux) Handle(name string, h Handler) error {
	if name == "" {
		return runlater.ErrEmptyName
	}
	if h == nil {
		return fmt.Errorf("httpjob: handler for %q is nil", name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[name]; exists {
		return fmt.Errorf("httpjob: handler already registered for %q", name)
	}
	m.handlers[name] = h
	return nil
}

// HandleJSON registers a typed JSON payload handler.
//
// A payload that does not fit T is a permanent failure: the same bytes will be
// redelivered and fail identically, so the decode error wraps ErrPermanent.
func HandleJSON[T any](m *Mux, name string, h func(context.Context, T) error) error {
	if h == nil {
		return fmt.Errorf("httpjob: handler for %q is nil", name)
	}
	return m.Handle(name, func(ctx context.Context, env runlater.Envelope) error {
		var payload T
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return fmt.Errorf("httpjob: decode %q payload: %w: %w", name, err, ErrPermanent)
		}
		return h(ctx, payload)
	})
}

// ServeHTTP validates the runlater protocol and invokes the registered handler.
// A handler error returns 500 so durable HTTP backends can retry the job,
// unless it wraps ErrPermanent.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		m.reportError(r, runlater.Envelope{}, fmt.Errorf("httpjob: method %s not allowed", r.Method))
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxEnvelopeBytes+1))
	if err != nil {
		m.reportError(r, runlater.Envelope{}, fmt.Errorf("httpjob: read body: %w", err))
		http.Error(w, "failed to read job", http.StatusBadRequest)
		return
	}
	if len(body) > maxEnvelopeBytes {
		m.reportError(r, runlater.Envelope{}, fmt.Errorf("httpjob: envelope exceeds %d bytes", maxEnvelopeBytes))
		http.Error(w, "job envelope too large", http.StatusRequestEntityTooLarge)
		return
	}

	env, err := runlater.DecodeEnvelope(body)
	if err != nil {
		m.reportError(r, runlater.Envelope{}, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.RLock()
	h := m.handlers[env.Name]
	m.mu.RUnlock()
	if h == nil {
		m.reportError(r, env, fmt.Errorf("httpjob: no handler registered for %q", env.Name))
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}

	if err := h(r.Context(), env); err != nil {
		m.reportError(r, env, err)
		if errors.Is(err, ErrPermanent) {
			// Report success so the provider stops retrying work that cannot
			// succeed. ErrorHandler has already seen the real failure.
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "job failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Mux) reportError(r *http.Request, env runlater.Envelope, err error) {
	if m.ErrorHandler != nil {
		m.ErrorHandler(r, env, err)
	}
}
