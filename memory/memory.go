// Package memory provides an in-process runlater backend for tests and local
// development. It is not durable: nothing survives the process.
package memory

import (
	"context"
	"sync"

	"github.com/kenkaton/runlater"
)

// Dispatcher stores jobs in memory for tests and local development. It is not durable.
type Dispatcher struct {
	// Deduplicate makes Dispatch emulate provider task-name deduplication: a
	// repeat of an already-seen (Name, ID) is not recorded again and its
	// receipt reports Deduplicated.
	//
	// Durable backends such as Cloud Tasks behave this way, and their
	// deduplication window outlives execution. Without this, a test suite
	// happily enqueues the same logical job twice and only production finds
	// out. Set it before the first Dispatch.
	Deduplicate bool

	mu   sync.Mutex
	jobs []runlater.Job
	seen map[string]bool
}

// Dispatch records job and returns immediately.
func (d *Dispatcher) Dispatch(_ context.Context, job runlater.Job) (runlater.Receipt, error) {
	if job.ID == "" {
		return runlater.Receipt{}, runlater.ErrEmptyID
	}
	if job.Name == "" {
		return runlater.Receipt{}, runlater.ErrEmptyName
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Deduplicate {
		key := job.Name + "\x00" + job.ID
		if d.seen[key] {
			return runlater.Receipt{ID: job.ID, ProviderID: job.ID, Deduplicated: true}, nil
		}
		if d.seen == nil {
			d.seen = make(map[string]bool)
		}
		d.seen[key] = true
	}

	copyJob := job
	copyJob.Payload = append([]byte(nil), job.Payload...)
	d.jobs = append(d.jobs, copyJob)
	return runlater.Receipt{ID: job.ID, ProviderID: job.ID}, nil
}

// Jobs returns a snapshot of all dispatched jobs.
func (d *Dispatcher) Jobs() []runlater.Job {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]runlater.Job, len(d.jobs))
	copy(out, d.jobs)
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
	}
	return out
}

// Reset removes all recorded jobs, including any deduplication history.
func (d *Dispatcher) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.jobs = nil
	d.seen = nil
}
