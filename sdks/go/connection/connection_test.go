package connection

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
	"github.com/gorilla/websocket"
)

func TestRequestAndQueryIDsIncrementFromZero(t *testing.T) {
	c := &Connection{}
	if got := c.NextRequestID(); got != 0 {
		t.Fatalf("unexpected first request id: %d", got)
	}
	if got := c.NextRequestID(); got != 1 {
		t.Fatalf("unexpected second request id: %d", got)
	}
	if got := c.NextQueryID(); got != 0 {
		t.Fatalf("unexpected first query id: %d", got)
	}
	if got := c.NextQueryID(); got != 1 {
		t.Fatalf("unexpected second query id: %d", got)
	}
}

func TestRoutePrecedenceRequestThenQueryThenKind(t *testing.T) {
	c := &Connection{}
	requestID := uint32(5)
	queryID := uint32(7)
	message := protocol.RoutedMessage{Kind: protocol.MessageKindReducerResult, RequestID: &requestID, QueryID: &queryID}

	var requestCalls atomic.Int32
	var queryCalls atomic.Int32
	var kindCalls atomic.Int32

	c.OnRequest(requestID, func(protocol.RoutedMessage) { requestCalls.Add(1) })
	c.OnQuery(queryID, func(protocol.RoutedMessage) { queryCalls.Add(1) })
	c.OnKind(protocol.MessageKindReducerResult, func(protocol.RoutedMessage) { kindCalls.Add(1) })

	if err := c.RouteMessage(message); err != nil {
		t.Fatalf("route message: %v", err)
	}

	if requestCalls.Load() != 1 || queryCalls.Load() != 0 || kindCalls.Load() != 0 {
		t.Fatalf("unexpected route invocation counts: request=%d query=%d kind=%d", requestCalls.Load(), queryCalls.Load(), kindCalls.Load())
	}
}

func TestRouteFallbacksAfterClearingRoutes(t *testing.T) {
	c := &Connection{}
	requestID := uint32(5)
	queryID := uint32(7)
	message := protocol.RoutedMessage{Kind: protocol.MessageKindReducerResult, RequestID: &requestID, QueryID: &queryID}

	var queryCalls atomic.Int32
	var kindCalls atomic.Int32

	c.OnRequest(requestID, func(protocol.RoutedMessage) { t.Fatalf("request route should have been cleared") })
	c.OnQuery(queryID, func(protocol.RoutedMessage) { queryCalls.Add(1) })
	c.OnKind(protocol.MessageKindReducerResult, func(protocol.RoutedMessage) { kindCalls.Add(1) })
	c.ClearRequestRoute(requestID)

	if err := c.RouteMessage(message); err != nil {
		t.Fatalf("route message: %v", err)
	}
	if queryCalls.Load() != 1 || kindCalls.Load() != 0 {
		t.Fatalf("unexpected route invocation counts after request clear: query=%d kind=%d", queryCalls.Load(), kindCalls.Load())
	}

	c.ClearQueryRoute(queryID)
	if err := c.RouteMessage(message); err != nil {
		t.Fatalf("route message: %v", err)
	}
	if queryCalls.Load() != 1 || kindCalls.Load() != 1 {
		t.Fatalf("unexpected route invocation counts after query clear: query=%d kind=%d", queryCalls.Load(), kindCalls.Load())
	}

	c.ClearKindRoute(protocol.MessageKindReducerResult)
	if err := c.RouteMessage(message); err != nil {
		t.Fatalf("route message: %v", err)
	}
	if kindCalls.Load() != 1 {
		t.Fatalf("kind route should not run after clear")
	}
}

func TestRouteMessageValidationFailure(t *testing.T) {
	c := &Connection{}
	if err := c.RouteMessage(protocol.RoutedMessage{}); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestDecompressServerMessage(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		raw := []byte{0, 'h', 'i'}
		decompressed, err := decompressServerMessage(raw)
		if err != nil {
			t.Fatalf("decompress: %v", err)
		}
		if !bytes.Equal(decompressed, []byte("hi")) {
			t.Fatalf("unexpected body: %q", string(decompressed))
		}

		raw[1] = 'H'
		if bytes.Equal(decompressed, raw[1:]) {
			t.Fatalf("expected returned body to be copied")
		}
	})

	t.Run("gzip", func(t *testing.T) {
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		_, _ = zw.Write([]byte("hello"))
		_ = zw.Close()

		raw := append([]byte{2}, zipped.Bytes()...)
		decompressed, err := decompressServerMessage(raw)
		if err != nil {
			t.Fatalf("decompress: %v", err)
		}
		if !bytes.Equal(decompressed, []byte("hello")) {
			t.Fatalf("unexpected gzip body: %q", string(decompressed))
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := []struct {
			name string
			raw  []byte
		}{
			{name: "empty", raw: []byte{}},
			{name: "brotli unsupported", raw: []byte{1, 1, 2, 3}},
			{name: "unknown scheme", raw: []byte{9, 1}},
			{name: "bad gzip", raw: []byte{2, 1, 2, 3}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := decompressServerMessage(tc.raw); err == nil {
					t.Fatalf("expected error")
				}
			})
		}
	})
}

func TestIsActiveReflectsClosedState(t *testing.T) {
	c := &Connection{}
	if !c.IsActive() {
		t.Fatalf("new connection without closed flag should be active")
	}
	c.closed.Store(true)
	if c.IsActive() {
		t.Fatalf("connection should be inactive after closed flag is set")
	}
}

func TestNotifyDisconnectCallsOnce(t *testing.T) {
	var calls atomic.Int32
	c := &Connection{onDisconnect: func(error) { calls.Add(1) }}
	c.notifyDisconnect(errors.New("first"))
	c.notifyDisconnect(errors.New("second"))
	if calls.Load() != 1 {
		t.Fatalf("disconnect callback should be called once, got %d", calls.Load())
	}
}

func TestCallReducerSendsAndDispatchesCallback(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	callbacks := make(chan error, 2)
	var requestID uint32
	requestID, err = c.CallReducer("set_name", []byte{1, 2, 3}, func(message protocol.RoutedMessage, callbackErr error) {
		if callbackErr != nil {
			callbacks <- callbackErr
			return
		}
		if message.RequestID == nil || *message.RequestID != requestID {
			callbacks <- errors.New("callback request id mismatch")
			return
		}
		callbacks <- nil
	})
	if err != nil {
		t.Fatalf("call reducer: %v", err)
	}
	if requestID != 0 {
		t.Fatalf("unexpected request id: %d", requestID)
	}

	select {
	case raw := <-incoming:
		var sent protocol.ClientMessage
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Fatalf("unmarshal outgoing reducer call: %v", err)
		}
		if sent.Kind != protocol.ClientMessageCallReducer || sent.Reducer != "set_name" || sent.RequestID != requestID {
			t.Fatalf("unexpected outbound reducer call: %+v", sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for outgoing reducer call")
	}

	msg := protocol.RoutedMessage{Kind: protocol.MessageKindReducerResult, RequestID: &requestID}
	if err := c.RouteMessage(msg); err != nil {
		t.Fatalf("route reducer result: %v", err)
	}

	select {
	case callbackErr := <-callbacks:
		if callbackErr != nil {
			t.Fatalf("unexpected callback error: %v", callbackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for reducer callback")
	}

	if err := c.RouteMessage(msg); err != nil {
		t.Fatalf("route reducer result second time: %v", err)
	}
	select {
	case callbackErr := <-callbacks:
		t.Fatalf("callback should not run twice, got err=%v", callbackErr)
	default:
	}
}

func TestCallProcedureUnexpectedResultKindReturnsCallbackError(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	callbacks := make(chan error, 1)
	requestID, err := c.CallProcedure("get_user", nil, func(_ protocol.RoutedMessage, callbackErr error) {
		callbacks <- callbackErr
	})
	if err != nil {
		t.Fatalf("call procedure: %v", err)
	}

	msg := protocol.RoutedMessage{Kind: protocol.MessageKindReducerResult, RequestID: &requestID}
	if err := c.RouteMessage(msg); err != nil {
		t.Fatalf("route unexpected procedure result: %v", err)
	}

	select {
	case callbackErr := <-callbacks:
		if callbackErr == nil || !strings.Contains(callbackErr.Error(), "unexpected result kind") {
			t.Fatalf("expected unexpected-kind callback error, got: %v", callbackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for procedure callback")
	}
}

func TestNotifyDisconnectFailsPendingCallCallbacks(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	callbacks := make(chan error, 1)
	if _, err := c.CallProcedure("get_user", nil, func(_ protocol.RoutedMessage, callbackErr error) {
		callbacks <- callbackErr
	}); err != nil {
		t.Fatalf("call procedure: %v", err)
	}

	c.notifyDisconnect(errors.New("socket closed"))

	select {
	case callbackErr := <-callbacks:
		if callbackErr == nil || !strings.Contains(callbackErr.Error(), "socket closed") {
			t.Fatalf("expected disconnect callback error, got: %v", callbackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for disconnect callback")
	}
}

func TestOneOffQuerySendsAndDispatchesCallback(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	callbacks := make(chan error, 1)
	requestID, err := c.OneOffQuery("select * from users", func(_ protocol.RoutedMessage, callbackErr error) {
		callbacks <- callbackErr
	})
	if err != nil {
		t.Fatalf("one off query: %v", err)
	}

	select {
	case raw := <-incoming:
		var sent protocol.ClientMessage
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Fatalf("unmarshal outgoing one-off query: %v", err)
		}
		if sent.Kind != protocol.ClientMessageOneOffQuery || sent.RequestID != requestID || sent.Query != "select * from users" {
			t.Fatalf("unexpected outbound one-off query: %+v", sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for outgoing one-off query")
	}

	msg := protocol.RoutedMessage{Kind: protocol.MessageKindOneOffQueryResult, RequestID: &requestID}
	if err := c.RouteMessage(msg); err != nil {
		t.Fatalf("route one-off query result: %v", err)
	}
	select {
	case callbackErr := <-callbacks:
		if callbackErr != nil {
			t.Fatalf("unexpected callback error: %v", callbackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for one-off callback")
	}
}

func TestSubscribeAndUnsubscribeMessageFlow(t *testing.T) {
	incoming := make(chan []byte, 2)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	subEvents := make(chan error, 4)
	queryID, err := c.Subscribe([]string{"select * from users"}, func(_ protocol.RoutedMessage, callbackErr error) {
		subEvents <- callbackErr
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if queryID != 0 {
		t.Fatalf("unexpected first query id: %d", queryID)
	}

	var subscribeMsg protocol.ClientMessage
	select {
	case raw := <-incoming:
		if err := json.Unmarshal(raw, &subscribeMsg); err != nil {
			t.Fatalf("unmarshal outgoing subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for subscribe message")
	}
	if subscribeMsg.Kind != protocol.ClientMessageSubscribe || subscribeMsg.QueryID == nil || *subscribeMsg.QueryID != queryID {
		t.Fatalf("unexpected subscribe message: %+v", subscribeMsg)
	}

	m1 := protocol.RoutedMessage{Kind: protocol.MessageKindSubscribeApplied, QueryID: &queryID}
	m2 := protocol.RoutedMessage{Kind: protocol.MessageKindTransactionUpdate, QueryID: &queryID}
	m3 := protocol.RoutedMessage{Kind: protocol.MessageKindUnsubscribeApplied, QueryID: &queryID}
	if err := c.RouteMessage(m1); err != nil {
		t.Fatalf("route subscribe_applied: %v", err)
	}
	if err := c.RouteMessage(m2); err != nil {
		t.Fatalf("route transaction_update: %v", err)
	}

	requestID, err := c.Unsubscribe(queryID)
	if err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if requestID <= subscribeMsg.RequestID {
		t.Fatalf("expected unsubscribe request id to advance, got %d <= %d", requestID, subscribeMsg.RequestID)
	}

	select {
	case raw := <-incoming:
		var unsubscribeMsg protocol.ClientMessage
		if err := json.Unmarshal(raw, &unsubscribeMsg); err != nil {
			t.Fatalf("unmarshal outgoing unsubscribe: %v", err)
		}
		if unsubscribeMsg.Kind != protocol.ClientMessageUnsubscribe || unsubscribeMsg.QueryID == nil || *unsubscribeMsg.QueryID != queryID {
			t.Fatalf("unexpected unsubscribe message: %+v", unsubscribeMsg)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for unsubscribe message")
	}

	if err := c.RouteMessage(m3); err != nil {
		t.Fatalf("route unsubscribe_applied: %v", err)
	}

	for i := 0; i < 3; i++ {
		select {
		case callbackErr := <-subEvents:
			if callbackErr != nil {
				t.Fatalf("unexpected subscription callback error: %v", callbackErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for subscription callback #%d", i+1)
		}
	}

	if err := c.RouteMessage(m2); err != nil {
		t.Fatalf("route transaction_update after unsubscribe: %v", err)
	}
	select {
	case callbackErr := <-subEvents:
		t.Fatalf("unexpected callback after unsubscribe: %v", callbackErr)
	default:
	}
}

func TestNotifyDisconnectFailsPendingSubscriptionCallbacks(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	callbacks := make(chan error, 1)
	if _, err := c.Subscribe([]string{"select * from users"}, func(_ protocol.RoutedMessage, callbackErr error) {
		callbacks <- callbackErr
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	c.notifyDisconnect(errors.New("socket closed"))

	select {
	case callbackErr := <-callbacks:
		if callbackErr == nil || !strings.Contains(callbackErr.Error(), "socket closed") {
			t.Fatalf("expected disconnect callback error, got: %v", callbackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for disconnect callback")
	}
}

func TestSubscribeSubscriptionErrorClearsRoute(t *testing.T) {
	incoming := make(chan []byte, 1)
	serverURL, cleanup := startWebsocketEchoSink(t, incoming)
	defer cleanup()

	c, err := buildTestConnection(t, serverURL)
	if err != nil {
		t.Fatalf("build test connection: %v", err)
	}
	defer c.Disconnect()

	subEvents := make(chan protocol.RoutedMessage, 4)
	queryID, err := c.Subscribe([]string{"select * from users"}, func(message protocol.RoutedMessage, callbackErr error) {
		if callbackErr != nil {
			t.Fatalf("unexpected subscription callback error: %v", callbackErr)
		}
		subEvents <- message
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	subscribeApplied := protocol.RoutedMessage{Kind: protocol.MessageKindSubscribeApplied, QueryID: &queryID}
	subscriptionError := protocol.RoutedMessage{Kind: protocol.MessageKindSubscriptionError, QueryID: &queryID}
	transactionUpdate := protocol.RoutedMessage{Kind: protocol.MessageKindTransactionUpdate, QueryID: &queryID}

	if err := c.RouteMessage(subscribeApplied); err != nil {
		t.Fatalf("route subscribe_applied: %v", err)
	}
	if err := c.RouteMessage(subscriptionError); err != nil {
		t.Fatalf("route subscription_error: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-subEvents:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for subscription callback #%d", i+1)
		}
	}

	if err := c.RouteMessage(transactionUpdate); err != nil {
		t.Fatalf("route transaction_update after subscription_error: %v", err)
	}
	select {
	case msg := <-subEvents:
		t.Fatalf("unexpected callback after subscription_error: kind=%s", msg.Kind)
	default:
	}
}

func TestInputValidationForQueryAndSubscriptionAPIs(t *testing.T) {
	c := &Connection{}

	if _, err := c.OneOffQuery("", nil); err == nil {
		t.Fatalf("expected empty one-off query to fail")
	}
	if _, err := c.Subscribe(nil, nil); err == nil {
		t.Fatalf("expected empty subscribe query list to fail")
	}
	if _, err := c.Subscribe([]string{""}, nil); err == nil {
		t.Fatalf("expected empty subscribe query string to fail")
	}
}

func TestFailedSendClearsPendingRequestCallbacks(t *testing.T) {
	c := &Connection{}
	c.closed.Store(true)
	c.messageEncoder = protocol.JSONMessageEncoder

	callbacks := make(chan error, 1)
	requestID, err := c.OneOffQuery("select * from users", func(_ protocol.RoutedMessage, callbackErr error) {
		callbacks <- callbackErr
	})
	if err == nil || !strings.Contains(err.Error(), "connection is closed") {
		t.Fatalf("expected send failure with closed connection, got: %v", err)
	}

	msg := protocol.RoutedMessage{
		Kind:      protocol.MessageKindOneOffQueryResult,
		RequestID: &requestID,
	}
	if err := c.RouteMessage(msg); err != nil {
		t.Fatalf("route one-off query result after send failure: %v", err)
	}
	select {
	case callbackErr := <-callbacks:
		t.Fatalf("callback should not be retained after send failure, got: %v", callbackErr)
	default:
	}
}

func buildTestConnection(t *testing.T, serverURL string) (*Connection, error) {
	t.Helper()
	wsURL := toWebsocketURL(t, serverURL)
	ws, err := dialWebsocket(t.Context(), wsURL, nil)
	if err != nil {
		return nil, err
	}
	return newConnection(ws, "cid", wsURL.String(), nil, nil, nil, nil), nil
}

func startWebsocketEchoSink(t *testing.T, incoming chan<- []byte) (string, func()) {
	t.Helper()

	upgrader := websocket.Upgrader{
		Subprotocols: []string{protocol.WSSubprotocolV2},
		CheckOrigin:  func(r *http.Request) bool { return true },
	}

	server := newLocalHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if messageType == websocket.BinaryMessage {
				incoming <- payload
			}
		}
	}))
	return server.URL, server.Close
}
