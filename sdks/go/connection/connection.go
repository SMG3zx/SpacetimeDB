package connection

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
	"github.com/gorilla/websocket"
)

type Connection struct {
	ws           *websocket.Conn
	connectionID string
	endpoint     string

	messageDecoder protocol.MessageDecoder
	messageEncoder protocol.MessageEncoder
	onMessage      func([]byte)
	onDisconnect   func(error)

	requestIDCounter atomic.Uint32
	queryIDCounter   atomic.Uint32

	requestRoutes sync.Map // map[uint32]protocol.RouteHandler
	queryRoutes   sync.Map // map[uint32]protocol.RouteHandler
	kindRoutes    sync.Map // map[protocol.MessageKind]protocol.RouteHandler
	callCallbacks sync.Map // map[uint32]callResultCallback
	subCallbacks  sync.Map // map[uint32]subscriptionCallback

	closed         atomic.Bool
	disconnectOnce sync.Once
	mu             sync.Mutex
}

func newConnection(
	ws *websocket.Conn,
	connectionID, endpoint string,
	messageDecoder protocol.MessageDecoder,
	messageEncoder protocol.MessageEncoder,
	onMessage func([]byte),
	onDisconnect func(error),
) *Connection {
	if messageEncoder == nil {
		messageEncoder = protocol.JSONMessageEncoder
	}

	return &Connection{
		ws:             ws,
		connectionID:   connectionID,
		endpoint:       endpoint,
		messageDecoder: messageDecoder,
		messageEncoder: messageEncoder,
		onMessage:      onMessage,
		onDisconnect:   onDisconnect,
	}
}

func (c *Connection) ConnectionID() string {
	return c.connectionID
}

func (c *Connection) Endpoint() string {
	return c.endpoint
}

func (c *Connection) IsActive() bool {
	return !c.closed.Load()
}

func (c *Connection) NextRequestID() uint32 {
	return c.requestIDCounter.Add(1) - 1
}

func (c *Connection) NextQueryID() uint32 {
	return c.queryIDCounter.Add(1) - 1
}

func (c *Connection) OnRequest(requestID uint32, handler protocol.RouteHandler) {
	c.requestRoutes.Store(requestID, handler)
}

func (c *Connection) OnQuery(queryID uint32, handler protocol.RouteHandler) {
	c.queryRoutes.Store(queryID, handler)
}

func (c *Connection) OnKind(kind protocol.MessageKind, handler protocol.RouteHandler) {
	c.kindRoutes.Store(kind, handler)
}

func (c *Connection) ClearRequestRoute(requestID uint32) {
	c.requestRoutes.Delete(requestID)
}

func (c *Connection) ClearQueryRoute(queryID uint32) {
	c.queryRoutes.Delete(queryID)
}

func (c *Connection) ClearKindRoute(kind protocol.MessageKind) {
	c.kindRoutes.Delete(kind)
}

func (c *Connection) RouteMessage(message protocol.RoutedMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	c.route(message)
	return nil
}

func (c *Connection) SendBinary(payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return wrapError(ErrorConnectionClosed, "send_binary", errors.New("connection is closed"))
	}
	return c.ws.WriteMessage(websocket.BinaryMessage, payload)
}

func (c *Connection) Disconnect() error {
	if c.closed.Swap(true) {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
	return c.ws.Close()
}

func (c *Connection) startReadLoop() {
	go func() {
		defer func() { _ = c.Disconnect() }()

		for {
			msgType, payload, err := c.ws.ReadMessage()
			if err != nil {
				c.notifyDisconnect(err)
				return
			}
			if msgType != websocket.BinaryMessage {
				continue
			}

			decompressed, err := decompressServerMessage(payload)
			if err != nil {
				c.notifyDisconnect(err)
				return
			}

			if c.onMessage != nil {
				c.onMessage(decompressed)
			}

			if c.messageDecoder != nil {
				message, err := c.messageDecoder(decompressed)
				if err != nil {
					c.notifyDisconnect(fmt.Errorf("decode incoming message: %w", err))
					return
				}
				if err := message.Validate(); err != nil {
					c.notifyDisconnect(fmt.Errorf("invalid routed message: %w", err))
					return
				}
				c.route(message)
			}
		}
	}()
}

func (c *Connection) notifyDisconnect(err error) {
	c.disconnectOnce.Do(func() {
		c.failPendingCalls(err)
		if c.onDisconnect != nil {
			c.onDisconnect(err)
		}
	})
}

func (c *Connection) route(message protocol.RoutedMessage) {
	if message.RequestID != nil {
		if handler, ok := c.requestRoutes.Load(*message.RequestID); ok {
			handler.(protocol.RouteHandler)(message)
			return
		}
	}
	if message.QueryID != nil {
		if handler, ok := c.queryRoutes.Load(*message.QueryID); ok {
			handler.(protocol.RouteHandler)(message)
			return
		}
	}
	if handler, ok := c.kindRoutes.Load(message.Kind); ok {
		handler.(protocol.RouteHandler)(message)
	}
}

func decompressServerMessage(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty websocket message")
	}

	scheme := payload[0]
	body := payload[1:]

	switch scheme {
	case 0:
		out := make([]byte, len(body))
		copy(out, body)
		return out, nil
	case 1:
		return nil, errors.New("brotli compression is not yet supported")
	case 2:
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer zr.Close()
		data, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("gzip decompress: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unknown compression scheme: %d", scheme)
	}
}

func (c *Connection) failPendingCalls(err error) {
	c.callCallbacks.Range(func(key, value any) bool {
		requestID, ok := key.(uint32)
		if !ok {
			return true
		}
		callback, ok := value.(callResultCallback)
		if !ok {
			return true
		}
		c.callCallbacks.Delete(requestID)
		c.ClearRequestRoute(requestID)
		callback(protocol.RoutedMessage{}, err)
		return true
	})

	c.subCallbacks.Range(func(key, value any) bool {
		queryID, ok := key.(uint32)
		if !ok {
			return true
		}
		callback, ok := value.(subscriptionCallback)
		if !ok {
			return true
		}
		c.subCallbacks.Delete(queryID)
		c.ClearQueryRoute(queryID)
		callback(protocol.RoutedMessage{}, err)
		return true
	})
}
