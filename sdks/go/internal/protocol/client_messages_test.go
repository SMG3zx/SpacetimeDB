package protocol

import (
	"encoding/json"
	"testing"
)

func TestJSONMessageEncoderEncodesExpectedFields(t *testing.T) {
	qid := uint32(9)
	encoded, err := JSONMessageEncoder(ClientMessage{
		Kind:      ClientMessageOneOffQuery,
		RequestID: 7,
		QueryID:   &qid,
		Query:     "select * from users",
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode encoded json: %v", err)
	}

	if got, ok := decoded["kind"].(string); !ok || got != string(ClientMessageOneOffQuery) {
		t.Fatalf("unexpected kind: %#v", decoded["kind"])
	}
	if got, ok := decoded["request_id"].(float64); !ok || got != 7 {
		t.Fatalf("unexpected request_id: %#v", decoded["request_id"])
	}
	if got, ok := decoded["query_id"].(float64); !ok || got != 9 {
		t.Fatalf("unexpected query_id: %#v", decoded["query_id"])
	}
}

func TestJSONMessageEncoderOmitsNilQueryID(t *testing.T) {
	encoded, err := JSONMessageEncoder(ClientMessage{
		Kind:      ClientMessageCallReducer,
		RequestID: 1,
		Reducer:   "set_name",
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode encoded json: %v", err)
	}

	if _, exists := decoded["query_id"]; exists {
		t.Fatalf("query_id should be omitted when nil")
	}
}
