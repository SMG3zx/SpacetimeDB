package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

type legacyIncomingMessage struct {
	Kind      string          `json:"kind"`
	RequestID *uint32         `json:"request_id"`
	QueryID   *uint32         `json:"query_id"`
	Payload   json.RawMessage `json:"payload"`
}

type taggedIncomingMessage struct {
	Tag   string          `json:"tag"`
	Value json.RawMessage `json:"value"`
}

// JSONMessageDecoder decodes JSON websocket payloads into RoutedMessage.
//
// It accepts two wire shapes:
// - Legacy envelope: {"kind":"reducer_result","request_id":1,"payload":{...}}
// - Tagged envelope: {"tag":"ReducerResult","value":{...}}
func JSONMessageDecoder(payload []byte) (RoutedMessage, error) {
	var legacy legacyIncomingMessage
	if err := json.Unmarshal(payload, &legacy); err == nil && legacy.Kind != "" {
		kind, ok := parseKindName(legacy.Kind)
		if !ok {
			kind = MessageKindUnknown
		}
		msg := RoutedMessage{
			Kind:      kind,
			RequestID: legacy.RequestID,
			QueryID:   legacy.QueryID,
		}
		if len(legacy.Payload) > 0 {
			var decoded any
			if err := json.Unmarshal(legacy.Payload, &decoded); err != nil {
				return RoutedMessage{}, fmt.Errorf("decode legacy payload: %w", err)
			}
			msg.Payload = decoded
		}
		if err := msg.Validate(); err != nil {
			return RoutedMessage{}, err
		}
		return msg, nil
	}

	var tagged taggedIncomingMessage
	if err := json.Unmarshal(payload, &tagged); err != nil {
		return RoutedMessage{}, fmt.Errorf("decode tagged message: %w", err)
	}
	if tagged.Tag == "" {
		return RoutedMessage{}, fmt.Errorf("missing message kind/tag")
	}

	kind, ok := taggedTagToKind(tagged.Tag)
	if !ok {
		kind = MessageKindUnknown
	}

	msg := RoutedMessage{Kind: kind}
	if len(tagged.Value) > 0 {
		var decoded any
		if err := json.Unmarshal(tagged.Value, &decoded); err != nil {
			return RoutedMessage{}, fmt.Errorf("decode tagged value: %w", err)
		}
		msg.Payload = decoded
		msg.RequestID = extractRequestID(decoded)
		msg.QueryID = extractQueryID(decoded)
	}

	if err := msg.Validate(); err != nil {
		return RoutedMessage{}, err
	}
	return msg, nil
}

func taggedTagToKind(tag string) (MessageKind, bool) {
	switch tag {
	case "InitialConnection":
		return MessageKindInitialConnection, true
	case "SubscribeApplied":
		return MessageKindSubscribeApplied, true
	case "UnsubscribeApplied":
		return MessageKindUnsubscribeApplied, true
	case "SubscriptionError":
		return MessageKindSubscriptionError, true
	case "TransactionUpdate":
		return MessageKindTransactionUpdate, true
	case "OneOffQueryResult":
		return MessageKindOneOffQueryResult, true
	case "ReducerResult":
		return MessageKindReducerResult, true
	case "ProcedureResult":
		return MessageKindProcedureResult, true
	default:
		return "", false
	}
}

func parseKindName(raw string) (MessageKind, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	normalized := strings.ToLower(raw)
	switch normalized {
	case string(MessageKindInitialConnection), "initialconnection":
		return MessageKindInitialConnection, true
	case string(MessageKindSubscribeApplied), "subscribeapplied":
		return MessageKindSubscribeApplied, true
	case string(MessageKindUnsubscribeApplied), "unsubscribeapplied":
		return MessageKindUnsubscribeApplied, true
	case string(MessageKindSubscriptionError), "subscriptionerror":
		return MessageKindSubscriptionError, true
	case string(MessageKindTransactionUpdate), "transactionupdate":
		return MessageKindTransactionUpdate, true
	case string(MessageKindOneOffQueryResult), "oneoffqueryresult":
		return MessageKindOneOffQueryResult, true
	case string(MessageKindReducerResult), "reducerresult":
		return MessageKindReducerResult, true
	case string(MessageKindProcedureResult), "procedureresult":
		return MessageKindProcedureResult, true
	default:
		return "", false
	}
}

func extractRequestID(payload any) *uint32 {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	val, ok := m["request_id"]
	if !ok {
		return nil
	}
	return floatToUint32Ptr(val)
}

func extractQueryID(payload any) *uint32 {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil
	}

	if raw, ok := m["query_id"]; ok {
		return floatToUint32Ptr(raw)
	}

	rawSet, ok := m["query_set_id"]
	if !ok {
		return nil
	}
	set, ok := rawSet.(map[string]any)
	if !ok {
		return nil
	}
	return floatToUint32Ptr(set["id"])
}

func floatToUint32Ptr(v any) *uint32 {
	switch n := v.(type) {
	case float64:
		out := uint32(n)
		return &out
	case int:
		out := uint32(n)
		return &out
	case uint32:
		out := n
		return &out
	default:
		return nil
	}
}
