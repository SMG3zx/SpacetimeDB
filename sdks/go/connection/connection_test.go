package connection

import (
	"bytes"
	"compress/gzip"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
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
