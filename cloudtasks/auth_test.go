package cloudtasks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestTokenSource(t *testing.T, h http.HandlerFunc) *MetadataTokenSource {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	s := NewMetadataTokenSource(server.Client())
	s.tokenURL = server.URL
	return s
}

func TestMetadataTokenCachesUntilExpiry(t *testing.T) {
	var calls int32
	s := newTestTokenSource(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Metadata-Flavor"); got != "Google" {
			t.Errorf("Metadata-Flavor = %q", got)
		}
		n := atomic.AddInt32(&calls, 1)
		fmt.Fprintf(w, `{"access_token":"token-%d","expires_in":3600,"token_type":"Bearer"}`, n)
	})

	for i := 0; i < 3; i++ {
		token, err := s.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if token != "token-1" {
			t.Fatalf("token = %q, want the cached token-1", token)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("metadata calls = %d, want 1", got)
	}
}

func TestMetadataTokenRefreshesInsideSkewWindow(t *testing.T) {
	var calls int32
	s := newTestTokenSource(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		fmt.Fprintf(w, `{"access_token":"token-%d","expires_in":3600}`, n)
	})
	if _, err := s.Token(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Expiry inside the skew window must not be served from cache.
	s.mu.Lock()
	s.expiresAt = time.Now().Add(expirySkew / 2)
	s.mu.Unlock()

	token, err := s.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "token-2" {
		t.Fatalf("token = %q, want a refreshed token", token)
	}
}

// A response without expires_in must not be cached under a guessed lifetime.
func TestMetadataTokenWithoutExpiryIsNotCached(t *testing.T) {
	var calls int32
	s := newTestTokenSource(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		fmt.Fprint(w, `{"access_token":"token"}`)
	})
	for i := 0; i < 2; i++ {
		if _, err := s.Token(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("metadata calls = %d, want 2", got)
	}
}

// Concurrent callers must collapse onto one refresh rather than stampeding the
// metadata server, and must not serialize behind a lock held across the request.
func TestMetadataTokenCollapsesConcurrentRefreshes(t *testing.T) {
	var calls int32
	s := newTestTokenSource(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(20 * time.Millisecond)
		fmt.Fprint(w, `{"access_token":"token","expires_in":3600}`)
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Token(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("metadata calls = %d, want 1", got)
	}
}

// One caller's slow request must not make another caller's cancellation wait.
func TestMetadataTokenWaiterHonoursContext(t *testing.T) {
	release := make(chan struct{})
	s := newTestTokenSource(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
		fmt.Fprint(w, `{"access_token":"token","expires_in":3600}`)
	})
	t.Cleanup(func() { close(release) })

	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = s.Token(context.Background())
	}()
	<-started
	// Give the first caller time to claim the refresh slot.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Token(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a context error")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter blocked behind an in-flight refresh")
	}
}

func TestMetadataTokenErrors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"non-2xx", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }},
		{"bad json", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `not json`) }},
		{"empty token", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"expires_in":3600}`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newTestTokenSource(t, tt.handler).Token(context.Background()); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestStaticTokenSource(t *testing.T) {
	if _, err := StaticTokenSource("").Token(context.Background()); err == nil {
		t.Fatal("expected error for empty static token")
	}
	got, err := StaticTokenSource("abc").Token(context.Background())
	if err != nil || got != "abc" {
		t.Fatalf("token = %q, err = %v", got, err)
	}
}

// The default metadata client must never route through a configured proxy:
// the endpoint is plain HTTP and the response carries an access token.
func TestDefaultMetadataClientHasNoProxy(t *testing.T) {
	tr, ok := newMetadataClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", newMetadataClient().Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("metadata transport must not use a proxy")
	}
}
