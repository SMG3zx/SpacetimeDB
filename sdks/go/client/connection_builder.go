package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/clockworklabs/spacetimedb-go/protocol"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// DbConnectionBuilder constructs a DbConnection with a fluent API.
type DbConnectionBuilder struct {
	host         string
	moduleName   string
	token        string
	compression  protocol.Compression
	onConnect    func(identity types.Identity, connectionId types.ConnectionId, token string)
	onDisconnect func(err error)
	onConnectErr func(err error)
}

// NewDbConnectionBuilder returns a builder with default settings (Brotli compression).
func NewDbConnectionBuilder() *DbConnectionBuilder {
	return &DbConnectionBuilder{
		compression: protocol.CompressionBrotli,
	}
}

// WithUri sets the server URI (e.g. "ws://localhost:3000" or "wss://example.com").
func (b *DbConnectionBuilder) WithUri(uri string) *DbConnectionBuilder {
	b.host = uri
	return b
}

// WithModuleName sets the SpacetimeDB module/database name.
func (b *DbConnectionBuilder) WithModuleName(name string) *DbConnectionBuilder {
	b.moduleName = name
	return b
}

// WithToken sets the authentication token.
func (b *DbConnectionBuilder) WithToken(token string) *DbConnectionBuilder {
	b.token = token
	return b
}

// WithCompression sets the preferred compression algorithm.
func (b *DbConnectionBuilder) WithCompression(c protocol.Compression) *DbConnectionBuilder {
	b.compression = c
	return b
}

// OnConnect registers a callback fired after InitialConnection is received.
func (b *DbConnectionBuilder) OnConnect(fn func(identity types.Identity, connectionId types.ConnectionId, token string)) *DbConnectionBuilder {
	b.onConnect = fn
	return b
}

// OnDisconnect registers a callback fired when the connection closes.
func (b *DbConnectionBuilder) OnDisconnect(fn func(err error)) *DbConnectionBuilder {
	b.onDisconnect = fn
	return b
}

// OnConnectError registers a callback fired if the initial connection fails.
func (b *DbConnectionBuilder) OnConnectError(fn func(err error)) *DbConnectionBuilder {
	b.onConnectErr = fn
	return b
}

// Build connects to SpacetimeDB and returns a ready DbConnection.
// It blocks until the InitialConnection server message is received.
func (b *DbConnectionBuilder) Build(ctx context.Context) (*DbConnection, error) {
	if b.host == "" {
		return nil, errors.New("DbConnectionBuilder: WithUri is required")
	}
	if b.moduleName == "" {
		return nil, errors.New("DbConnectionBuilder: WithModuleName is required")
	}

	wsURL, err := buildWsURL(b.host, b.moduleName, b.compression)
	if err != nil {
		return nil, fmt.Errorf("DbConnectionBuilder: %w", err)
	}

	headers := make(http.Header)
	if b.token != "" {
		headers.Set("Authorization", "Bearer "+b.token)
	}

	ws, err := Dial(ctx, wsURL, headers)
	if err != nil {
		if b.onConnectErr != nil {
			b.onConnectErr(err)
		}
		return nil, err
	}

	// Wait for InitialConnection.
	msg := <-ws.Recv()
	if msg.Err != nil {
		ws.Close()
		if b.onConnectErr != nil {
			b.onConnectErr(msg.Err)
		}
		return nil, msg.Err
	}
	if msg.Message.Kind != protocol.ServerMessageInitialConnection {
		err := fmt.Errorf("expected InitialConnection, got %d", msg.Message.Kind)
		ws.Close()
		if b.onConnectErr != nil {
			b.onConnectErr(err)
		}
		return nil, err
	}

	ic := msg.Message.InitialConnection
	conn := newDbConnection(ws, ic.Identity, ic.ConnectionId, ic.Token, b.onDisconnect)

	if b.onConnect != nil {
		b.onConnect(ic.Identity, ic.ConnectionId, ic.Token)
	}

	return conn, nil
}

func buildWsURL(host, moduleName string, compression protocol.Compression) (string, error) {
	base, err := url.Parse(host)
	if err != nil {
		return "", fmt.Errorf("invalid host URL: %w", err)
	}

	// Ensure ws/wss scheme.
	switch base.Scheme {
	case "http":
		base.Scheme = "ws"
	case "https":
		base.Scheme = "wss"
	case "ws", "wss":
		// already correct
	default:
		return "", fmt.Errorf("unsupported scheme %q", base.Scheme)
	}

	base.Path = fmt.Sprintf("/v1/database/%s/subscribe", moduleName)

	q := base.Query()
	switch compression {
	case protocol.CompressionBrotli:
		q.Set("compression", "Brotli")
	case protocol.CompressionGzip:
		q.Set("compression", "Gzip")
	}
	base.RawQuery = q.Encode()

	return base.String(), nil
}
