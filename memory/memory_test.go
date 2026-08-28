package memory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/kenkaton/runlater"
)

func TestDispatchRecordsJobs(t *testing.T) {
	d := &Dispatcher{}
	c := runlater.New(d)
	if _, err := c.Do(context.Background(), "email.send", map[string]int{"user_id": 42}, runlater.ID("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(context.Background(), "email.send", nil, runlater.ID("b")); err != nil {
		t.Fatal(err)
	}
	jobs := d.Jobs()
	if len(jobs) != 2 || jobs[0].ID != "a" || jobs[1].ID != "b" {
		t.Fatalf("jobs = %+v", jobs)
	}
	if string(jobs[0].Payload) != `{"user_id":42}` {
		t.Fatalf("payload = %s", jobs[0].Payload)
	}
}

// Jobs must hand out copies: a caller mutating the snapshot must not corrupt
// what the next assertion sees.
func TestJobsReturnsIndependentCopies(t *testing.T) {
	d := &Dispatcher{}
	if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "a", Name: "x", Payload: []byte(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	first := d.Jobs()
	first[0].Payload[2] = 'X'
	first[0].ID = "mutated"
	if second := d.Jobs(); string(second[0].Payload) != `{"n":1}` || second[0].ID != "a" {
		t.Fatalf("snapshot leaked mutation: %+v", second[0])
	}
}

// The point of the option: the same failure production would show, in a test.
func TestDeduplicateMatchesProviderSemantics(t *testing.T) {
	d := &Dispatcher{Deduplicate: true}
	c := runlater.New(d)

	first, err := c.Do(context.Background(), "email.send", nil, runlater.ID("welcome-42"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Deduplicated {
		t.Fatal("first handoff must not be deduplicated")
	}

	second, err := c.Do(context.Background(), "email.send", nil, runlater.ID("welcome-42"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Deduplicated {
		t.Fatal("repeat handoff must be deduplicated")
	}
	if got := len(d.Jobs()); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}

	// The same local ID under a different job name is a different job.
	if _, err := c.Do(context.Background(), "audit.write", nil, runlater.ID("welcome-42")); err != nil {
		t.Fatal(err)
	}
	if got := len(d.Jobs()); got != 2 {
		t.Fatalf("jobs = %d, want 2", got)
	}
}

func TestDeduplicateOffAllowsRepeats(t *testing.T) {
	d := &Dispatcher{}
	for i := 0; i < 2; i++ {
		if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "a", Name: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(d.Jobs()); got != 2 {
		t.Fatalf("jobs = %d, want 2", got)
	}
}

func TestResetClearsJobsAndDeduplication(t *testing.T) {
	d := &Dispatcher{Deduplicate: true}
	if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "a", Name: "x"}); err != nil {
		t.Fatal(err)
	}
	d.Reset()
	if got := len(d.Jobs()); got != 0 {
		t.Fatalf("jobs = %d, want 0", got)
	}
	receipt, err := d.Dispatch(context.Background(), runlater.Job{ID: "a", Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Deduplicated {
		t.Fatal("Reset must clear deduplication history")
	}
}

func TestDispatchRequiresIdentity(t *testing.T) {
	d := &Dispatcher{}
	if _, err := d.Dispatch(context.Background(), runlater.Job{Name: "x"}); !errors.Is(err, runlater.ErrEmptyID) {
		t.Fatalf("err = %v", err)
	}
	if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "a"}); !errors.Is(err, runlater.ErrEmptyName) {
		t.Fatalf("err = %v", err)
	}
	if got := len(d.Jobs()); got != 0 {
		t.Fatalf("jobs = %d, want 0", got)
	}
}

func TestConcurrentDispatch(t *testing.T) {
	d := &Dispatcher{Deduplicate: true}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.Dispatch(context.Background(), runlater.Job{ID: "same", Name: "x"}); err != nil {
				t.Error(err)
			}
			d.Jobs()
		}()
	}
	wg.Wait()
	if got := len(d.Jobs()); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}
}
