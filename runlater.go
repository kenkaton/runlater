package runlater

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrNoDispatcher = errors.New("runlater: dispatcher is nil")
	ErrEmptyName    = errors.New("runlater: job name is empty")
)

// Job is the runlater handoff contract. Backends may add stronger guarantees,
// but must preserve ID, Name, Payload, and RunAt semantics.
type Job struct {
	ID      string
	Name    string
	Payload json.RawMessage
	RunAt   time.Time
}

// Receipt identifies the provider-side handoff created for a job.
type Receipt struct {
	ID         string
	ProviderID string
}

// Dispatcher hands a job off to a backend. A successful return means the
// backend accepted responsibility for the job according to its documented
// delivery guarantees.
type Dispatcher interface {
	Dispatch(context.Context, Job) (Receipt, error)
}

// Client turns application values into provider-neutral jobs.
type Client struct {
	dispatcher Dispatcher
	now        func() time.Time
	newID      func() (string, error)
}

// New creates a Client backed by d.
func New(d Dispatcher) *Client {
	return &Client{dispatcher: d, now: time.Now, newID: randomID}
}

type options struct {
	id       string
	delay    time.Duration
	runAt    time.Time
	hasDelay bool
	hasRunAt bool
}

// Option configures a job before it is dispatched.
type Option func(*options) error

// ID gives the job a stable logical identifier. Reusing the same ID lets
// backends that support deduplication make retries safer.
func ID(id string) Option {
	return func(o *options) error {
		if id == "" {
			return errors.New("runlater: job ID is empty")
		}
		o.id = id
		return nil
	}
}

// After schedules the job after d has elapsed.
func After(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("runlater: negative delay: %s", d)
		}
		if o.hasDelay {
			return errors.New("runlater: After specified more than once")
		}
		o.delay = d
		o.hasDelay = true
		return nil
	}
}

// At schedules the job for t.
func At(t time.Time) Option {
	return func(o *options) error {
		if o.hasRunAt {
			return errors.New("runlater: At specified more than once")
		}
		o.runAt = t
		o.hasRunAt = true
		return nil
	}
}

// Do serializes payload as JSON and hands the job to the configured Dispatcher.
// It returns only after the backend has accepted responsibility for the job.
func (c *Client) Do(ctx context.Context, name string, payload any, opts ...Option) (Receipt, error) {
	if c == nil || c.dispatcher == nil {
		return Receipt{}, ErrNoDispatcher
	}
	if name == "" {
		return Receipt{}, ErrEmptyName
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return Receipt{}, fmt.Errorf("runlater: marshal payload: %w", err)
	}

	var cfg options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return Receipt{}, err
		}
	}
	if cfg.hasRunAt && cfg.hasDelay {
		return Receipt{}, errors.New("runlater: After and At cannot be used together")
	}

	id := cfg.id
	if id == "" {
		id, err = c.newID()
		if err != nil {
			return Receipt{}, fmt.Errorf("runlater: generate job ID: %w", err)
		}
	}

	runAt := cfg.runAt
	if cfg.hasDelay {
		runAt = c.now().Add(cfg.delay)
	}

	return c.dispatcher.Dispatch(ctx, Job{
		ID:      id,
		Name:    name,
		Payload: body,
		RunAt:   runAt,
	})
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
