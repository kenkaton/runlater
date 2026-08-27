package runlater

import (
	"encoding/json"
	"fmt"
)

// ProtocolVersion is the current runlater wire protocol version.
const ProtocolVersion = 1

// Envelope is the stable wire format handed to HTTP-style job targets.
type Envelope struct {
	Version int             `json:"version"`
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

// EncodeEnvelope serializes a Job using the runlater wire protocol.
func EncodeEnvelope(job Job) ([]byte, error) {
	if job.ID == "" {
		return nil, fmt.Errorf("runlater: envelope job ID is empty")
	}
	if job.Name == "" {
		return nil, ErrEmptyName
	}
	return json.Marshal(Envelope{
		Version: ProtocolVersion,
		ID:      job.ID,
		Name:    job.Name,
		Payload: job.Payload,
	})
}

// DecodeEnvelope parses and validates a runlater wire envelope.
func DecodeEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("runlater: decode envelope: %w", err)
	}
	if env.Version != ProtocolVersion {
		return Envelope{}, fmt.Errorf("runlater: unsupported protocol version %d", env.Version)
	}
	if env.ID == "" {
		return Envelope{}, fmt.Errorf("runlater: envelope job ID is empty")
	}
	if env.Name == "" {
		return Envelope{}, ErrEmptyName
	}
	return env, nil
}
