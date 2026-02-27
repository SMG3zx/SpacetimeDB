package protocol

import "testing"

func TestJSONMessageDecoderLegacyEnvelope(t *testing.T) {
	msg, err := JSONMessageDecoder([]byte(`{"kind":"reducer_result","request_id":7,"payload":{"ok":true}}`))
	if err != nil {
		t.Fatalf("decode legacy message: %v", err)
	}
	if msg.Kind != MessageKindReducerResult {
		t.Fatalf("unexpected kind: %s", msg.Kind)
	}
	if msg.RequestID == nil || *msg.RequestID != 7 {
		t.Fatalf("unexpected request id: %+v", msg.RequestID)
	}
}

func TestJSONMessageDecoderLegacyEnvelopePascalKind(t *testing.T) {
	msg, err := JSONMessageDecoder([]byte(`{"kind":"ReducerResult","request_id":7,"payload":{"ok":true}}`))
	if err != nil {
		t.Fatalf("decode legacy pascal-kind message: %v", err)
	}
	if msg.Kind != MessageKindReducerResult {
		t.Fatalf("unexpected kind: %s", msg.Kind)
	}
}

func TestJSONMessageDecoderTaggedEnvelope(t *testing.T) {
	msg, err := JSONMessageDecoder([]byte(`{"tag":"SubscribeApplied","value":{"request_id":9,"query_set_id":{"id":3},"rows":{"tables":[]}}}`))
	if err != nil {
		t.Fatalf("decode tagged message: %v", err)
	}
	if msg.Kind != MessageKindSubscribeApplied {
		t.Fatalf("unexpected kind: %s", msg.Kind)
	}
	if msg.QueryID == nil || *msg.QueryID != 3 {
		t.Fatalf("unexpected query id: %+v", msg.QueryID)
	}
}

func TestJSONMessageDecoderRejectsUnknownShape(t *testing.T) {
	if _, err := JSONMessageDecoder([]byte(`{"value":{}}`)); err == nil {
		t.Fatalf("expected unknown shape decode to fail")
	}
}
