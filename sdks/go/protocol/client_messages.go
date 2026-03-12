package protocol

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// BinProtocol is the Sec-WebSocket-Protocol header value for v2 BSATN.
const BinProtocol = "v2.bsatn.spacetimedb"

// --- Client -> Server ---

// ClientMessageKind is the Sum type tag for ClientMessage variants.
type ClientMessageKind uint8

const (
	ClientMessageSubscribe      ClientMessageKind = 0
	ClientMessageUnsubscribe    ClientMessageKind = 1
	ClientMessageOneOffQuery    ClientMessageKind = 2
	ClientMessageCallReducer    ClientMessageKind = 3
	ClientMessageCallProcedure  ClientMessageKind = 4
)

type SubscribeMsg struct {
	RequestId    uint32
	QuerySetId   QuerySetId
	QueryStrings []string
}

type UnsubscribeMsg struct {
	RequestId  uint32
	QuerySetId QuerySetId
	Flags      UnsubscribeFlags
}

type UnsubscribeFlags uint8

const (
	UnsubscribeFlagsDefault      UnsubscribeFlags = 0
	UnsubscribeFlagsSendDropped  UnsubscribeFlags = 1
)

type OneOffQueryMsg struct {
	RequestId   uint32
	QueryString string
}

type CallReducerMsg struct {
	RequestId uint32
	Flags     uint8
	Reducer   string
	Args      []byte
}

type CallProcedureMsg struct {
	RequestId uint32
	Flags     uint8
	Procedure string
	Args      []byte
}

// ClientMessage is the union of all client-to-server messages.
type ClientMessage struct {
	Kind         ClientMessageKind
	Subscribe    *SubscribeMsg
	Unsubscribe  *UnsubscribeMsg
	OneOffQuery  *OneOffQueryMsg
	CallReducer  *CallReducerMsg
	CallProcedure *CallProcedureMsg
}

// WriteClientMessage encodes a ClientMessage into BSATN.
func WriteClientMessage(w *bsatn.Writer, msg ClientMessage) error {
	w.WriteVariantTag(uint8(msg.Kind))
	switch msg.Kind {
	case ClientMessageSubscribe:
		m := msg.Subscribe
		w.WriteU32(m.RequestId)
		writeQuerySetId(w, m.QuerySetId)
		bsatn.WriteSlice(w, m.QueryStrings, func(w *bsatn.Writer, s string) { w.WriteString(s) })
	case ClientMessageUnsubscribe:
		m := msg.Unsubscribe
		w.WriteU32(m.RequestId)
		writeQuerySetId(w, m.QuerySetId)
		w.WriteU8(uint8(m.Flags))
	case ClientMessageOneOffQuery:
		m := msg.OneOffQuery
		w.WriteU32(m.RequestId)
		w.WriteString(m.QueryString)
	case ClientMessageCallReducer:
		m := msg.CallReducer
		w.WriteU32(m.RequestId)
		w.WriteU8(m.Flags)
		w.WriteString(m.Reducer)
		w.WriteBytes(m.Args)
	case ClientMessageCallProcedure:
		m := msg.CallProcedure
		w.WriteU32(m.RequestId)
		w.WriteU8(m.Flags)
		w.WriteString(m.Procedure)
		w.WriteBytes(m.Args)
	default:
		return fmt.Errorf("protocol: unknown client message kind %d", msg.Kind)
	}
	return nil
}

// EncodeClientMessage returns the BSATN encoding of msg.
func EncodeClientMessage(msg ClientMessage) ([]byte, error) {
	w := bsatn.NewWriter()
	if err := WriteClientMessage(w, msg); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
