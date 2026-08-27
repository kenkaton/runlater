package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const metadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

// MetadataTokenSource gets and caches OAuth2 access tokens from the Google Cloud metadata server.
type MetadataTokenSource struct {
	client *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// NewMetadataTokenSource creates a metadata-server token source.
func NewMetadataTokenSource(client *http.Client) *MetadataTokenSource {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &MetadataTokenSource{client: client}
}

type metadataTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// Token returns a cached token when possible and refreshes it before expiry.
func (s *MetadataTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token != "" && time.Now().Add(time.Minute).Before(s.expiresAt) {
		return s.token, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataTokenURL, nil)
	if err != nil {
		return "", fmt.Errorf("metadata token: create request: %w", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata token: request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("metadata token: unexpected status %s", resp.Status)
	}

	var out metadataTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("metadata token: decode response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("metadata token: empty access token")
	}

	s.token = out.AccessToken
	s.expiresAt = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	return s.token, nil
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
