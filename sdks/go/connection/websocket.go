package connection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SMG3zx/SpacetimeDB/sdks/go/internal/protocol"
	"github.com/gorilla/websocket"
)

type websocketTokenResponse struct {
	Token string `json:"token"`
}

func exchangeWebsocketToken(ctx context.Context, host *url.URL, authToken string) (string, error) {
	tokenURL := *host
	switch tokenURL.Scheme {
	case "wss":
		tokenURL.Scheme = "https"
	case "ws":
		tokenURL.Scheme = "http"
	}
	tokenURL.Path = "/v1/identity/websocket-token"
	tokenURL.RawQuery = ""
	tokenURL.Fragment = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("build websocket-token request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request websocket-token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("websocket-token request failed: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var decoded websocketTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode websocket-token response: %w", err)
	}
	if decoded.Token == "" {
		return "", fmt.Errorf("websocket-token response missing token")
	}
	return decoded.Token, nil
}

func dialWebsocket(ctx context.Context, endpoint *url.URL, headers map[string]string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		Subprotocols: []string{protocol.WSSubprotocolV2},
	}

	httpHeader := http.Header{}
	for k, v := range headers {
		httpHeader.Set(k, v)
	}

	conn, resp, err := dialer.DialContext(ctx, endpoint.String(), httpHeader)
	if err != nil {
		if resp != nil && resp.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			trimmed := strings.TrimSpace(string(body))
			if trimmed != "" {
				return nil, fmt.Errorf("websocket dial failed: %w (status=%d body=%q)", err, resp.StatusCode, trimmed)
			}
			return nil, fmt.Errorf("websocket dial failed: %w (status=%d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}

	if !strings.EqualFold(conn.Subprotocol(), protocol.WSSubprotocolV2) {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected websocket subprotocol: got %q want %q", conn.Subprotocol(), protocol.WSSubprotocolV2)
	}

	if err := conn.WriteControl(websocket.PingMessage, bytes.Repeat([]byte{0}, 1), time.Now().Add(websocket.DefaultDialer.HandshakeTimeout)); err != nil {
		// Ping failure right after connect usually means the socket is already unhealthy.
		_ = conn.Close()
		return nil, fmt.Errorf("websocket post-connect ping failed: %w", err)
	}

	return conn, nil
}
