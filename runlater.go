// Package runlater is a provider-native durable handoff primitive.
//
// It owns one small boundary: turning an application value into a
// provider-neutral [Job] and handing that job to a [Dispatcher]. Durable
// storage, retry timing, and worker execution belong to the backend's
// provider, not to this package.
//
// A successful [Client.Do] means only that the selected backend accepted
// responsibility for the job according to that backend's documented
// guarantees. It does not promise exactly-once execution: providers may
// deliver a job more than once, so handlers must be idempotent.
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
	// ErrNoDispatcher is returned when a Client has no backend to hand jobs to.
	ErrNoDispatcher = errors.New("runlater: dispatcher is nil")
	// ErrEmptyName is returned when a job has no name.
	ErrEmptyName = errors.New("runlater: job name is empty")
	// ErrEmptyID is returned when a job has no identifier.
	ErrEmptyID = errors.New("runlater: job ID is empty")
	// ErrInvalidOption wraps every Option misuse, so callers can match all of
	// them with errors.Is without depending on individual messages.
	ErrInvalidOption = errors.New("runlater: invalid option")
	// ErrAmbiguousHandoff marks a dispatch attempt whose outcome is unknown.
	// The provider may already have accepted the job even though the caller did
	// not receive a successful response. Retry the same logical ID rather than
	// creating a new one.
	ErrAmbiguousHandoff = errors.New("runlater: ambiguous handoff outcome")
)

// Job is the runlater handoff contract. Backends may add stronger guarantees,
// but must preserve ID, Name, Payload, and RunAt semantics.
//
// A zero RunAt means "as soon as the provider can run it".
type Job struct {
	ID      string
	Name    string
	Payload json.RawMessage
	RunAt   time.Time
}

// Receipt identifies a handoff attempt.
//
// On success, ProviderID identifies the provider-side job that accepted
// responsibility. On error, fields may still be populated so callers can
// safely reason about or retry an uncertain handoff; their presence does not
// imply that the provider accepted the job.
type Receipt struct {
	ID         string
	ProviderID string

	// Deduplicated reports that the backend recognized this handoff as one it
	// had already accepted, so no new provider-side job was created. Backends
	// that cannot distinguish the two cases always leave it false.
	//
	// This matters because provider deduplication windows outlive execution:
	// a Deduplicated receipt can mean the job already ran. Callers that need a
	// job to run again must use a different logical ID.
	Deduplicated bool
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
	hasID    bool
	hasDelay bool
	hasRunAt bool
}

// Option configures a job before it is dispatched.
type Option func(*options) error

// ID gives the job a stable logical identifier. Reusing the same ID lets
// backends that support deduplication make ambiguous enqueue retries safer.
func ID(id string) Option {
	return func(o *options) error {
		if id == "" {
			return fmt.Errorf("%w: %w", ErrInvalidOption, ErrEmptyID)
		}
		if o.hasID {
			return fmt.Errorf("%w: ID specified more than once", ErrInvalidOption)
		}
		o.id = id
		o.hasID = true
		return nil
	}
}

// After schedules the job after d has elapsed.
func After(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("%w: negative delay: %s", ErrInvalidOption, d)
		}
		if o.hasDelay {
			return fmt.Errorf("%w: After specified more than once", ErrInvalidOption)
		}
		o.delay = d
		o.hasDelay = true
		return nil
	}
}

// At schedules the job for t.
//
// A zero t is rejected rather than treated as "run immediately", because an
// unset time is far more often an unpopulated struct field than a deliberate
// request. Omit At entirely to run the job as soon as the provider can.
func At(t time.Time) Option {
	return func(o *options) error {
		if t.IsZero() {
			return fmt.Errorf("%w: At time is zero", ErrInvalidOption)
		}
		if o.hasRunAt {
			return fmt.Errorf("%w: At specified more than once", ErrInvalidOption)
		}
		o.runAt = t
		o.hasRunAt = true
		return nil
	}
}

// Do serializes payload as JSON and hands the job to the configured Dispatcher.
// It returns only after the backend has accepted responsibility for the job.
//
// If dispatch fails after the logical ID has been chosen, the returned Receipt
// still contains that ID. This is especially important for generated IDs: a
// caller can retain the identity of an uncertain attempt instead of retrying
// with a new logical job by accident.
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
			return Receipt{}, fmt.Errorf("%w: nil option", ErrInvalidOption)
		}
		if err := opt(&cfg); err != nil {
			return Receipt{}, err
		}
	}
	if cfg.hasRunAt && cfg.hasDelay {
		return Receipt{}, fmt.Errorf("%w: After and At cannot be used together", ErrInvalidOption)
	}

	id := cfg.id
	if !cfg.hasID {
		id, err = c.newID()
		if err != nil {
			return Receipt{}, fmt.Errorf("runlater: generate job ID: %w", err)
		}
	}

	runAt := cfg.runAt
	if cfg.hasDelay {
		runAt = c.now().Add(cfg.delay)
	}

	receipt, err := c.dispatcher.Dispatch(ctx, Job{
		ID:      id,
		Name:    name,
		Payload: body,
		RunAt:   runAt,
	})
	if err != nil && receipt.ID == "" {
		receipt.ID = id
	}
	return receipt, err
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
