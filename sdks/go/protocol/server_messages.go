package protocol

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// --- Server -> Client ---

// ServerMessageKind is the Sum type tag for ServerMessage variants.
type ServerMessageKind uint8

const (
	ServerMessageInitialConnection  ServerMessageKind = 0
	ServerMessageSubscribeApplied   ServerMessageKind = 1
	ServerMessageUnsubscribeApplied ServerMessageKind = 2
	ServerMessageSubscriptionError  ServerMessageKind = 3
	ServerMessageTransactionUpdate  ServerMessageKind = 4
	ServerMessageOneOffQueryResult  ServerMessageKind = 5
	ServerMessageReducerResult      ServerMessageKind = 6
	ServerMessageProcedureResult    ServerMessageKind = 7
)

type InitialConnectionMsg struct {
	Identity     types.Identity
	ConnectionId types.ConnectionId
	Token        string
}

type SubscribeAppliedMsg struct {
	RequestId  uint32
	QuerySetId QuerySetId
	Rows       QueryRows
}

type UnsubscribeAppliedMsg struct {
	RequestId  uint32
	QuerySetId QuerySetId
	Rows       *QueryRows // nil if not requested
}

type SubscriptionErrorMsg struct {
	RequestId  *uint32 // nil if not triggered by a specific request
	QuerySetId QuerySetId
	Error      string
}

type TransactionUpdateMsg struct {
	TransactionUpdate TransactionUpdate
}

type OneOffQueryResultMsg struct {
	RequestId uint32
	Rows      *QueryRows // nil on error
	Err       string     // non-empty on error
}

type ReducerResultMsg struct {
	RequestId uint32
	Timestamp types.Timestamp
	Result    ReducerOutcome
}

type ProcedureResultMsg struct {
	RequestId                  uint32
	Timestamp                  types.Timestamp
	TotalHostExecutionDuration types.TimeDuration
	Status                     ProcedureStatus
}

// ServerMessage is the union of all server-to-client messages.
type ServerMessage struct {
	Kind                ServerMessageKind
	InitialConnection   *InitialConnectionMsg
	SubscribeApplied    *SubscribeAppliedMsg
	UnsubscribeApplied  *UnsubscribeAppliedMsg
	SubscriptionError   *SubscriptionErrorMsg
	TransactionUpdate   *TransactionUpdateMsg
	OneOffQueryResult   *OneOffQueryResultMsg
	ReducerResult       *ReducerResultMsg
	ProcedureResult     *ProcedureResultMsg
}

// ReadServerMessage decodes a ServerMessage from BSATN bytes.
func ReadServerMessage(r *bsatn.Reader) (ServerMessage, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return ServerMessage{}, err
	}
	kind := ServerMessageKind(tag)
	switch kind {
	case ServerMessageInitialConnection:
		identity, err := types.ReadIdentity(r)
		if err != nil {
			return ServerMessage{}, err
		}
		connId, err := types.ReadConnectionId(r)
		if err != nil {
			return ServerMessage{}, err
		}
		token, err := r.ReadString()
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, InitialConnection: &InitialConnectionMsg{
			Identity: identity, ConnectionId: connId, Token: token,
		}}, nil

	case ServerMessageSubscribeApplied:
		reqId, err := r.ReadU32()
		if err != nil {
			return ServerMessage{}, err
		}
		qsid, err := readQuerySetId(r)
		if err != nil {
			return ServerMessage{}, err
		}
		rows, err := readQueryRows(r)
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, SubscribeApplied: &SubscribeAppliedMsg{
			RequestId: reqId, QuerySetId: qsid, Rows: rows,
		}}, nil

	case ServerMessageUnsubscribeApplied:
		reqId, err := r.ReadU32()
		if err != nil {
			return ServerMessage{}, err
		}
		qsid, err := readQuerySetId(r)
		if err != nil {
			return ServerMessage{}, err
		}
		rows, err := bsatn.ReadOption(r, readQueryRows)
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, UnsubscribeApplied: &UnsubscribeAppliedMsg{
			RequestId: reqId, QuerySetId: qsid, Rows: rows,
		}}, nil

	case ServerMessageSubscriptionError:
		reqId, err := bsatn.ReadOption(r, func(r *bsatn.Reader) (uint32, error) { return r.ReadU32() })
		if err != nil {
			return ServerMessage{}, err
		}
		qsid, err := readQuerySetId(r)
		if err != nil {
			return ServerMessage{}, err
		}
		errMsg, err := r.ReadString()
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, SubscriptionError: &SubscriptionErrorMsg{
			RequestId: reqId, QuerySetId: qsid, Error: errMsg,
		}}, nil

	case ServerMessageTransactionUpdate:
		tx, err := readTransactionUpdate(r)
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, TransactionUpdate: &TransactionUpdateMsg{TransactionUpdate: tx}}, nil

	case ServerMessageOneOffQueryResult:
		reqId, err := r.ReadU32()
		if err != nil {
			return ServerMessage{}, err
		}
		// Result<QueryRows, string> — tag 0 = Ok, tag 1 = Err
		resultTag, err := r.ReadVariantTag()
		if err != nil {
			return ServerMessage{}, err
		}
		msg := &OneOffQueryResultMsg{RequestId: reqId}
		switch resultTag {
		case 0:
			rows, err := readQueryRows(r)
			if err != nil {
				return ServerMessage{}, err
			}
			msg.Rows = &rows
		case 1:
			errStr, err := r.ReadString()
			if err != nil {
				return ServerMessage{}, err
			}
			msg.Err = errStr
		default:
			return ServerMessage{}, fmt.Errorf("protocol: unknown OneOffQueryResult tag %d", resultTag)
		}
		return ServerMessage{Kind: kind, OneOffQueryResult: msg}, nil

	case ServerMessageReducerResult:
		reqId, err := r.ReadU32()
		if err != nil {
			return ServerMessage{}, err
		}
		ts, err := types.ReadTimestamp(r)
		if err != nil {
			return ServerMessage{}, err
		}
		outcome, err := readReducerOutcome(r)
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, ReducerResult: &ReducerResultMsg{
			RequestId: reqId, Timestamp: ts, Result: outcome,
		}}, nil

	case ServerMessageProcedureResult:
		status, err := readProcedureStatus(r)
		if err != nil {
			return ServerMessage{}, err
		}
		ts, err := types.ReadTimestamp(r)
		if err != nil {
			return ServerMessage{}, err
		}
		dur, err := types.ReadTimeDuration(r)
		if err != nil {
			return ServerMessage{}, err
		}
		reqId, err := r.ReadU32()
		if err != nil {
			return ServerMessage{}, err
		}
		return ServerMessage{Kind: kind, ProcedureResult: &ProcedureResultMsg{
			RequestId: reqId, Timestamp: ts, TotalHostExecutionDuration: dur, Status: status,
		}}, nil

	default:
		return ServerMessage{}, fmt.Errorf("protocol: unknown server message tag %d", tag)
	}
}

// DecodeServerMessage decompresses a raw WebSocket frame then decodes the ServerMessage.
func DecodeServerMessage(frame []byte) (ServerMessage, error) {
	payload, err := DecompressServerMessage(frame)
	if err != nil {
		return ServerMessage{}, err
	}
	return ReadServerMessage(bsatn.NewReader(payload))
}
