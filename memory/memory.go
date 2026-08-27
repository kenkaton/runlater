package memory

import (
	"context"
	"sync"

	"github.com/kenkaton/runlater"
)

// Dispatcher stores jobs in memory for tests and local development.
type Dispatcher struct {
	mu   sync.Mutex
	jobs []runlater.Job
}

// Dispatch records job and returns immediately.
func (d *Dispatcher) Dispatch(_ context.Context, job runlater.Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	copyJob := job
	copyJob.Payload = append([]byte(nil), job.Payload...)
	d.jobs = append(d.jobs, copyJob)
	return nil
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

// Reset removes all recorded jobs.
func (d *Dispatcher) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.jobs = nil
}
