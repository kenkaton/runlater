package runlater

import (
	"context"
	"errors"
	"testing"
	"time"
)

type captureDispatcher struct {
	job Job
}

func (d *captureDispatcher) Dispatch(_ context.Context, job Job) error {
	d.job = job
	return nil
}

func TestDo(t *testing.T) {
	d := &captureDispatcher{}
	c := New(d)
	c.now = func() time.Time { return time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC) }

	if err := c.Do(context.Background(), "email.send", map[string]int{"user_id": 42}, After(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if d.job.Name != "email.send" {
		t.Fatalf("name = %q", d.job.Name)
	}
	if got, want := string(d.job.Payload), `{"user_id":42}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
	wantRunAt := time.Date(2026, 8, 28, 0, 2, 0, 0, time.UTC)
	if !d.job.RunAt.Equal(wantRunAt) {
		t.Fatalf("runAt = %s, want %s", d.job.RunAt, wantRunAt)
	}
}

func TestDoRejectsAtAndAfterTogether(t *testing.T) {
	c := New(&captureDispatcher{})
	err := c.Do(context.Background(), "job", nil, At(time.Now()), After(time.Second))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoRequiresDispatcher(t *testing.T) {
	err := New(nil).Do(context.Background(), "job", nil)
	if !errors.Is(err, ErrNoDispatcher) {
		t.Fatalf("err = %v", err)
	}
}
