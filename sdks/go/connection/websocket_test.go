package connection

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
	"github.com/gorilla/websocket"
)

func TestExchangeWebsocketToken(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method: %s", r.Method)
			}
			if r.URL.Path != "/v1/identity/websocket-token" {
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer auth-token" {
				t.Fatalf("unexpected auth header: %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"ws-token"}`))
		}))
		defer server.Close()

		host, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse host: %v", err)
		}

		token, err := exchangeWebsocketToken(context.Background(), host, "auth-token")
		if err != nil {
			t.Fatalf("exchange token: %v", err)
		}
		if token != "ws-token" {
			t.Fatalf("unexpected token: %q", token)
		}
	})

	t.Run("error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusUnauthorized)
		}))
		defer server.Close()

		host, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse host: %v", err)
		}

		_, err = exchangeWebsocketToken(context.Background(), host, "auth-token")
		if err == nil || !strings.Contains(err.Error(), "status=401") {
			t.Fatalf("expected status error, got: %v", err)
		}
	})

	t.Run("missing token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"token":""}`))
		}))
		defer server.Close()

		host, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse host: %v", err)
		}

		_, err = exchangeWebsocketToken(context.Background(), host, "auth-token")
		if err == nil || !strings.Contains(err.Error(), "missing token") {
			t.Fatalf("expected missing token error, got: %v", err)
		}
	})
}

func TestDialWebsocket(t *testing.T) {
	t.Run("success with expected subprotocol", func(t *testing.T) {
		upgrader := websocket.Upgrader{
			Subprotocols: []string{protocol.WSSubprotocolV2},
			CheckOrigin:  func(r *http.Request) bool { return true },
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}))
		defer server.Close()

		wsURL := toWebsocketURL(t, server.URL)
		conn, err := dialWebsocket(context.Background(), wsURL, map[string]string{"X-Test": "1"})
		if err != nil {
			t.Fatalf("dial websocket: %v", err)
		}
		_ = conn.Close()
	})

	t.Run("subprotocol mismatch", func(t *testing.T) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}))
		defer server.Close()

		wsURL := toWebsocketURL(t, server.URL)
		conn, err := dialWebsocket(context.Background(), wsURL, nil)
		if conn != nil {
			_ = conn.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "unexpected websocket subprotocol") {
			t.Fatalf("expected subprotocol mismatch error, got: %v", err)
		}
	})
}

func toWebsocketURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		t.Fatalf("unexpected test server scheme: %s", u.Scheme)
	}
	u.Path = "/"
	if u.RawPath != "" {
		u.RawPath = "/"
	}
	u.RawQuery = ""
	u.Fragment = ""
	if u.Host == "" {
		t.Fatalf("unexpected empty host in %q", raw)
	}
	return u
}
