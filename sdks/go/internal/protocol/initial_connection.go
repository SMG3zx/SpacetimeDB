package protocol

import (
	"encoding/json"
	"fmt"
)

// InitialConnectionPayload is the payload shape for the "initial_connection" server message.
type InitialConnectionPayload struct {
	Identity     string `json:"identity"`
	ConnectionID string `json:"connection_id"`
	Token        string `json:"token"`
}

// DecodeInitialConnectionPayload decodes a routed message payload into InitialConnectionPayload.
//
// Supported payload input forms:
// - InitialConnectionPayload
// - map[string]any
// - []byte containing JSON
func DecodeInitialConnectionPayload(payload any) (InitialConnectionPayload, error) {
	switch p := payload.(type) {
	case nil:
		return InitialConnectionPayload{}, fmt.Errorf("initial_connection payload is nil")
	case InitialConnectionPayload:
		return validateInitialConnectionPayload(p)
	case map[string]any:
		var decoded InitialConnectionPayload
		raw, err := json.Marshal(p)
		if err != nil {
			return InitialConnectionPayload{}, fmt.Errorf("marshal initial_connection payload map: %w", err)
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return InitialConnectionPayload{}, fmt.Errorf("decode initial_connection payload map: %w", err)
		}
		return validateInitialConnectionPayload(decoded)
	case []byte:
		var decoded InitialConnectionPayload
		if err := json.Unmarshal(p, &decoded); err != nil {
			return InitialConnectionPayload{}, fmt.Errorf("decode initial_connection payload bytes: %w", err)
		}
		return validateInitialConnectionPayload(decoded)
	default:
		return InitialConnectionPayload{}, fmt.Errorf("unsupported initial_connection payload type %T", payload)
	}
}

func validateInitialConnectionPayload(payload InitialConnectionPayload) (InitialConnectionPayload, error) {
	if payload.Identity == "" {
		return InitialConnectionPayload{}, fmt.Errorf("initial_connection payload missing identity")
	}
	if payload.ConnectionID == "" {
		return InitialConnectionPayload{}, fmt.Errorf("initial_connection payload missing connection_id")
	}
	if payload.Token == "" {
		return InitialConnectionPayload{}, fmt.Errorf("initial_connection payload missing token")
	}
	return payload, nil
}

