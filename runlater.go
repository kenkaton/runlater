package runlater

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrNoDispatcher = errors.New("runlater: dispatcher is nil")
	ErrEmptyName    = errors.New("runlater: job name is empty")
)

// Job is the provider-neutral representation of work that should run later.
type Job struct {
	Name    string
	Payload json.RawMessage
	RunAt   time.Time
}

// Dispatcher persists a job for later execution.
type Dispatcher interface {
	Dispatch(context.Context, Job) error
}

// Client turns application values into provider-neutral jobs.
type Client struct {
	dispatcher Dispatcher
	now        func() time.Time
}

// New creates a Client backed by d.
func New(d Dispatcher) *Client {
	return &Client{dispatcher: d, now: time.Now}
}

type options struct {
	delay time.Duration
	runAt time.Time
}

// Option configures a job before it is dispatched.
type Option func(*options) error

// After schedules the job after d has elapsed.
func After(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("runlater: negative delay: %s", d)
		}
		o.delay = d
		return nil
	}
}

// At schedules the job for t.
func At(t time.Time) Option {
	return func(o *options) error {
		o.runAt = t
		return nil
	}
}

// Do serializes payload as JSON and hands the job to the configured Dispatcher.
func (c *Client) Do(ctx context.Context, name string, payload any, opts ...Option) error {
	if c == nil || c.dispatcher == nil {
		return ErrNoDispatcher
	}
	if name == "" {
		return ErrEmptyName
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("runlater: marshal payload: %w", err)
	}

	var cfg options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return err
		}
	}
	if !cfg.runAt.IsZero() && cfg.delay != 0 {
		return errors.New("runlater: After and At cannot be used together")
	}

	runAt := cfg.runAt
	if cfg.delay > 0 {
		runAt = c.now().Add(cfg.delay)
	}

	return c.dispatcher.Dispatch(ctx, Job{
		Name:    name,
		Payload: body,
		RunAt:   runAt,
	})
}
