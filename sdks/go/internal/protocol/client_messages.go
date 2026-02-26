package protocol

import "encoding/json"

type ClientMessageKind string

const (
	ClientMessageCallReducer   ClientMessageKind = "call_reducer"
	ClientMessageCallProcedure ClientMessageKind = "call_procedure"
	ClientMessageOneOffQuery   ClientMessageKind = "one_off_query"
	ClientMessageSubscribe     ClientMessageKind = "subscribe_multi"
	ClientMessageUnsubscribe   ClientMessageKind = "unsubscribe_multi"
)

type ClientMessage struct {
	Kind         ClientMessageKind `json:"kind"`
	RequestID    uint32            `json:"request_id"`
	QueryID      *uint32           `json:"query_id,omitempty"`
	Reducer      string            `json:"reducer,omitempty"`
	Procedure    string            `json:"procedure,omitempty"`
	Args         []byte            `json:"args,omitempty"`
	Query        string            `json:"query,omitempty"`
	QueryStrings []string          `json:"query_strings,omitempty"`
}

type MessageEncoder func(ClientMessage) ([]byte, error)

func JSONMessageEncoder(message ClientMessage) ([]byte, error) {
	return json.Marshal(message)
}

