package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	metadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
	// expirySkew is how long before expiry a cached token stops being used.
	expirySkew = time.Minute
)

// MetadataTokenSource gets and caches OAuth2 access tokens from the Google Cloud metadata server.
type MetadataTokenSource struct {
	client *http.Client
	// tokenURL is overridden by tests; empty means the real metadata server.
	tokenURL string

	// refresh admits one refresher at a time. It is a channel rather than a
	// mutex so that waiters can honour their context instead of blocking
	// behind someone else's request.
	refresh chan struct{}

	mu        sync.Mutex // guards token and expiresAt only, never held across I/O
	token     string
	expiresAt time.Time
}

// NewMetadataTokenSource creates a metadata-server token source.
//
// When client is nil a dedicated client is used whose transport has no proxy.
// That matters: the metadata endpoint is plain HTTP, and a transport honouring
// HTTP_PROXY would send the request, and the access token it returns, through
// whatever host that variable names.
func NewMetadataTokenSource(client *http.Client) *MetadataTokenSource {
	if client == nil {
		client = newMetadataClient()
	}
	return &MetadataTokenSource{client: client, refresh: make(chan struct{}, 1)}
}

func newMetadataClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}

type metadataTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// Token returns a cached token when possible and refreshes it before expiry.
func (s *MetadataTokenSource) Token(ctx context.Context) (string, error) {
	if token, ok := s.cached(); ok {
		return token, nil
	}

	select {
	case s.refresh <- struct{}{}:
		defer func() { <-s.refresh }()
	case <-ctx.Done():
		return "", fmt.Errorf("metadata token: %w", ctx.Err())
	}

	// Someone else may have refreshed while we waited for our turn.
	if token, ok := s.cached(); ok {
		return token, nil
	}

	token, expiresAt, err := s.fetch(ctx)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	s.token, s.expiresAt = token, expiresAt
	s.mu.Unlock()
	return token, nil
}

func (s *MetadataTokenSource) cached() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" {
		return "", false
	}
	return s.token, time.Now().Add(expirySkew).Before(s.expiresAt)
}

// fetch returns a token and the instant it stops being usable. A response
// without a positive expires_in yields a zero expiry, which keeps the token
// out of the cache: guessing a lifetime we were not told is worse than paying
// for another request.
func (s *MetadataTokenSource) fetch(ctx context.Context) (string, time.Time, error) {
	url := s.tokenURL
	if url == "" {
		url = metadataTokenURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("metadata token: create request: %w", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("metadata token: request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", time.Time{}, fmt.Errorf("metadata token: unexpected status %s", resp.Status)
	}

	var out metadataTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", time.Time{}, fmt.Errorf("metadata token: decode response: %w", err)
	}
	if out.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("metadata token: empty access token")
	}

	var expiresAt time.Time
	if out.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	}
	return out.AccessToken, expiresAt, nil
}

// StaticTokenSource is useful for tests and local tooling that already has an access token.
type StaticTokenSource string

// Token returns the configured static access token.
func (s StaticTokenSource) Token(context.Context) (string, error) {
	if s == "" {
		return "", fmt.Errorf("cloudtasks: static token is empty")
	}
	return string(s), nil
}
