package client

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/protocol"
	"github.com/coder/websocket"
)

const (
	keepaliveInterval = 30 * time.Second
	dialTimeout       = 30 * time.Second
)

// ParsedMessage holds either a decoded ServerMessage or an error.
type ParsedMessage struct {
	Message protocol.ServerMessage
	Err     error
}

// WsConnection wraps a WebSocket connection and provides a message-based API.
type WsConnection struct {
	conn     *websocket.Conn
	incoming chan ParsedMessage
	outgoing chan []byte
	once     sync.Once
	done     chan struct{}
}

// Dial opens a WebSocket connection to the SpacetimeDB server.
// url must be a ws:// or wss:// URL.
// headers are merged with the required protocol header.
func Dial(ctx context.Context, url string, headers http.Header) (*WsConnection, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	opts := &websocket.DialOptions{
		Subprotocols: []string{protocol.BinProtocol},
		HTTPHeader:   headers,
	}

	conn, _, err := websocket.Dial(dialCtx, url, opts)
	if err != nil {
		return nil, fmt.Errorf("ws dial %s: %w", url, err)
	}

	// Allow large messages (modules can send large initial row sets).
	conn.SetReadLimit(64 * 1024 * 1024) // 64 MiB

	ws := &WsConnection{
		conn:     conn,
		incoming: make(chan ParsedMessage, 64),
		outgoing: make(chan []byte, 64),
		done:     make(chan struct{}),
	}

	go ws.readLoop(context.Background())
	go ws.writeLoop(context.Background())
	go ws.keepalive(context.Background())

	return ws, nil
}

// Recv returns the channel on which decoded server messages arrive.
func (ws *WsConnection) Recv() <-chan ParsedMessage {
	return ws.incoming
}

// Send encodes msg as BSATN and queues it for transmission.
func (ws *WsConnection) Send(msg protocol.ClientMessage) error {
	data, err := protocol.EncodeClientMessage(msg)
	if err != nil {
		return err
	}
	select {
	case ws.outgoing <- data:
		return nil
	case <-ws.done:
		return fmt.Errorf("connection closed")
	}
}

// Close shuts down the connection gracefully.
func (ws *WsConnection) Close() error {
	ws.once.Do(func() { close(ws.done) })
	return ws.conn.Close(websocket.StatusNormalClosure, "bye")
}

func (ws *WsConnection) readLoop(ctx context.Context) {
	defer close(ws.incoming)
	for {
		msgType, data, err := ws.conn.Read(ctx)
		if err != nil {
			select {
			case ws.incoming <- ParsedMessage{Err: err}:
			case <-ws.done:
			}
			return
		}
		if msgType != websocket.MessageBinary {
			continue // ignore non-binary frames
		}
		msg, err := protocol.DecodeServerMessage(data)
		select {
		case ws.incoming <- ParsedMessage{Message: msg, Err: err}:
		case <-ws.done:
			return
		}
	}
}

func (ws *WsConnection) writeLoop(ctx context.Context) {
	for {
		select {
		case data := <-ws.outgoing:
			if err := ws.conn.Write(ctx, websocket.MessageBinary, data); err != nil {
				return
			}
		case <-ws.done:
			return
		}
	}
}

func (ws *WsConnection) keepalive(ctx context.Context) {
	t := time.NewTicker(keepaliveInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := ws.conn.Ping(ctx); err != nil {
				return
			}
		case <-ws.done:
			return
		}
	}
}

// EncodeSubscribe is a convenience helper for building subscribe messages.
func EncodeSubscribe(requestId uint32, querySetId uint32, queries []string) ([]byte, error) {
	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageSubscribe,
		Subscribe: &protocol.SubscribeMsg{
			RequestId:    requestId,
			QuerySetId:   protocol.QuerySetId{ID: querySetId},
			QueryStrings: queries,
		},
	}
	return protocol.EncodeClientMessage(msg)
}

// EncodeCallReducer is a convenience helper for building reducer call messages.
func EncodeCallReducer(requestId uint32, reducer string, argsBsatn []byte) ([]byte, error) {
	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageCallReducer,
		CallReducer: &protocol.CallReducerMsg{
			RequestId: requestId,
			Flags:     0,
			Reducer:   reducer,
			Args:      argsBsatn,
		},
	}
	return protocol.EncodeClientMessage(msg)
}

// EncodeUnsubscribe is a convenience helper for building unsubscribe messages.
func EncodeUnsubscribe(requestId, querySetId uint32) ([]byte, error) {
	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageUnsubscribe,
		Unsubscribe: &protocol.UnsubscribeMsg{
			RequestId:  requestId,
			QuerySetId: protocol.QuerySetId{ID: querySetId},
			Flags:      protocol.UnsubscribeFlagsDefault,
		},
	}
	return protocol.EncodeClientMessage(msg)
}

// ProductValueWriter helps build BSATN-encoded ProductValue args for reducers.
type ProductValueWriter struct {
	w *bsatn.Writer
}

// NewProductValueWriter returns a new writer for reducer arguments.
func NewProductValueWriter() *ProductValueWriter {
	return &ProductValueWriter{w: bsatn.NewWriter()}
}

// Bytes returns the encoded product value bytes.
func (p *ProductValueWriter) Bytes() []byte { return p.w.Bytes() }

// Writer returns the underlying bsatn.Writer for direct writes.
func (p *ProductValueWriter) Writer() *bsatn.Writer { return p.w }
