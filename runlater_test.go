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

type failingDispatcher struct {
	job Job
	err error
}

func (d *failingDispatcher) Dispatch(_ context.Context, job Job) (Receipt, error) {
	d.job = job
	return Receipt{}, d.err
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

func TestDoPreservesGeneratedIDWhenDispatchFails(t *testing.T) {
	wantErr := errors.New("network outcome unknown")
	d := &failingDispatcher{err: wantErr}
	c := New(d)
	c.newID = func() (string, error) { return "generated", nil }

	receipt, err := c.Do(context.Background(), "job", nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if receipt.ID != "generated" {
		t.Fatalf("receipt ID = %q, want generated", receipt.ID)
	}
	if d.job.ID != receipt.ID {
		t.Fatalf("dispatched ID = %q, receipt ID = %q", d.job.ID, receipt.ID)
	}
}

func TestDoPreservesDispatcherReceiptOnError(t *testing.T) {
	wantErr := errors.New("ambiguous")
	d := DispatcherFunc(func(_ context.Context, job Job) (Receipt, error) {
		return Receipt{ID: job.ID, ProviderID: "provider/known"}, wantErr
	})

	receipt, err := New(d).Do(context.Background(), "job", nil, ID("stable"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if receipt.ID != "stable" || receipt.ProviderID != "provider/known" {
		t.Fatalf("receipt = %+v", receipt)
	}
}

// DispatcherFunc adapts a function to Dispatcher for focused contract tests.
type DispatcherFunc func(context.Context, Job) (Receipt, error)

func (f DispatcherFunc) Dispatch(ctx context.Context, job Job) (Receipt, error) {
	return f(ctx, job)
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

func TestAtSchedulesExplicitTime(t *testing.T) {
	d := &captureDispatcher{}
	runAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, err := New(d).Do(context.Background(), "job", nil, At(runAt)); err != nil {
		t.Fatal(err)
	}
	if !d.job.RunAt.Equal(runAt) {
		t.Fatalf("runAt = %s, want %s", d.job.RunAt, runAt)
	}
}

// A zero time must not silently degrade into "run immediately": that turns an
// unpopulated struct field into an instantly executed job.
func TestAtRejectsZeroTime(t *testing.T) {
	d := &captureDispatcher{}
	_, err := New(d).Do(context.Background(), "job", nil, At(time.Time{}))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("err = %v, want ErrInvalidOption", err)
	}
	if d.job.Name != "" {
		t.Fatal("job must not be dispatched")
	}
}

func TestOptionErrors(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		opts []Option
		want error
	}{
		{"empty ID", []Option{ID("")}, ErrEmptyID},
		{"duplicate ID", []Option{ID("a"), ID("b")}, ErrInvalidOption},
		{"negative delay", []Option{After(-time.Second)}, ErrInvalidOption},
		{"duplicate After", []Option{After(time.Second), After(time.Minute)}, ErrInvalidOption},
		{"duplicate At", []Option{At(now), At(now)}, ErrInvalidOption},
		{"At and After", []Option{At(now), After(0)}, ErrInvalidOption},
		{"nil option", []Option{nil}, ErrInvalidOption},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &captureDispatcher{}
			_, err := New(d).Do(context.Background(), "job", nil, tt.opts...)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if d.job.Name != "" {
				t.Fatal("job must not be dispatched")
			}
		})
	}
}

func TestDoRequiresName(t *testing.T) {
	if _, err := New(&captureDispatcher{}).Do(context.Background(), "", nil); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeEnvelopeDefaultsMissingPayloadToNull(t *testing.T) {
	env, err := DecodeEnvelope([]byte(`{"version":1,"id":"id","name":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(env.Payload) != "null" {
		t.Fatalf("payload = %q, want null", env.Payload)
	}
}

func TestEncodeEnvelopeRequiresIdentity(t *testing.T) {
	if _, err := EncodeEnvelope(Job{Name: "x"}); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("err = %v", err)
	}
	if _, err := EncodeEnvelope(Job{ID: "id"}); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("err = %v", err)
	}
}
