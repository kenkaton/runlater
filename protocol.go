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
		return nil, ErrEmptyID
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
//
// A missing payload decodes to JSON null rather than to nil, so that handlers
// see a decodable document instead of failing on empty input. Producers that
// omit the field would otherwise cause a permanently undecodable job that a
// durable backend retries until the queue gives up.
func DecodeEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("runlater: decode envelope: %w", err)
	}
	if env.Version != ProtocolVersion {
		return Envelope{}, fmt.Errorf("runlater: unsupported protocol version %d", env.Version)
	}
	if env.ID == "" {
		return Envelope{}, ErrEmptyID
	}
	if env.Name == "" {
		return Envelope{}, ErrEmptyName
	}
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("null")
	}
	return env, nil
}
