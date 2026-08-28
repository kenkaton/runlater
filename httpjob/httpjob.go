// Package httpjob adapts the runlater wire protocol to net/http handlers.
package httpjob

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/kenkaton/runlater"
)

const maxEnvelopeBytes = 1 << 20

// Handler processes one decoded runlater envelope.
type Handler func(context.Context, runlater.Envelope) error

// Mux routes runlater envelopes by job name. It is safe for concurrent use.
type Mux struct {
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
func HandleJSON[T any](m *Mux, name string, h func(context.Context, T) error) error {
	if h == nil {
		return fmt.Errorf("httpjob: handler for %q is nil", name)
	}
	return m.Handle(name, func(ctx context.Context, env runlater.Envelope) error {
		var payload T
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return fmt.Errorf("httpjob: decode %q payload: %w", name, err)
		}
		return h(ctx, payload)
	})
}

// ServeHTTP validates the runlater protocol and invokes the registered handler.
// A handler error returns 500 so durable HTTP backends can retry the job.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxEnvelopeBytes+1))
	if err != nil {
		http.Error(w, "failed to read job", http.StatusBadRequest)
		return
	}
	if len(body) > maxEnvelopeBytes {
		http.Error(w, "job envelope too large", http.StatusRequestEntityTooLarge)
		return
	}

	env, err := runlater.DecodeEnvelope(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.RLock()
	h := m.handlers[env.Name]
	m.mu.RUnlock()
	if h == nil {
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}

	if err := h(r.Context(), env); err != nil {
		http.Error(w, "job failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
