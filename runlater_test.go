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

func (d *captureDispatcher) Dispatch(_ context.Context, job Job) (Receipt, error) {
	d.job = job
	return Receipt{ID: job.ID, ProviderID: "provider/" + job.ID}, nil
}

func TestDo(t *testing.T) {
	d := &captureDispatcher{}
	c := New(d)
	c.now = func() time.Time { return time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC) }

	receipt, err := c.Do(context.Background(), "email.send", map[string]int{"user_id": 42}, ID("welcome-42"), After(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != "welcome-42" {
		t.Fatalf("receipt ID = %q", receipt.ID)
	}
	if d.job.ID != "welcome-42" || d.job.Name != "email.send" {
		t.Fatalf("job = %+v", d.job)
	}
	if got, want := string(d.job.Payload), `{"user_id":42}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
	wantRunAt := time.Date(2026, 8, 28, 0, 2, 0, 0, time.UTC)
	if !d.job.RunAt.Equal(wantRunAt) {
		t.Fatalf("runAt = %s, want %s", d.job.RunAt, wantRunAt)
	}
}

func TestDoGeneratesID(t *testing.T) {
	d := &captureDispatcher{}
	c := New(d)
	c.newID = func() (string, error) { return "generated", nil }
	if _, err := c.Do(context.Background(), "job", nil); err != nil {
		t.Fatal(err)
	}
	if d.job.ID != "generated" {
		t.Fatalf("ID = %q", d.job.ID)
	}
}

func TestDoRejectsAtAndAfterTogetherEvenZeroDelay(t *testing.T) {
	c := New(&captureDispatcher{})
	_, err := c.Do(context.Background(), "job", nil, At(time.Now()), After(0))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoRequiresDispatcher(t *testing.T) {
	_, err := New(nil).Do(context.Background(), "job", nil)
	if !errors.Is(err, ErrNoDispatcher) {
		t.Fatalf("err = %v", err)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	job := Job{ID: "id-1", Name: "email.send", Payload: []byte(`{"user_id":42}`)}
	data, err := EncodeEnvelope(job)
	if err != nil {
		t.Fatal(err)
	}
	env, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if env.Version != ProtocolVersion || env.ID != job.ID || env.Name != job.Name {
		t.Fatalf("env = %+v", env)
	}
}
