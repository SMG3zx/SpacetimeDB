package connection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"

	"github.com/SMG3zx/SpacetimeDB/sdks/go/internal/protocol"
)

type Builder struct {
	uri               string
	databaseName      string
	token             string
	compression       protocol.Compression
	messageDecoder    protocol.MessageDecoder
	messageEncoder    protocol.MessageEncoder
	lightMode         bool
	confirmedReads    *bool
	useWebsocketToken bool
	onConnect         func(*Connection)
	onConnectError    func(error)
	onDisconnect      func(error)
	onMessage         func([]byte)
}

func NewBuilder() *Builder {
	return &Builder{
		compression:       protocol.CompressionGzip,
		messageDecoder:    protocol.JSONMessageDecoder,
		useWebsocketToken: true,
	}
}

func (b *Builder) WithURI(uri string) *Builder {
	b.uri = uri
	return b
}

// WithURL is an alias for WithURI using idiomatic Go acronym casing.
func (b *Builder) WithURL(uri string) *Builder {
	return b.WithURI(uri)
}

func (b *Builder) WithDatabaseName(name string) *Builder {
	b.databaseName = name
	return b
}

func (b *Builder) WithToken(token string) *Builder {
	b.token = token
	return b
}

func (b *Builder) WithCompression(compression protocol.Compression) *Builder {
	b.compression = compression
	return b
}

func (b *Builder) WithMessageDecoder(decoder protocol.MessageDecoder) *Builder {
	b.messageDecoder = decoder
	return b
}

func (b *Builder) WithMessageEncoder(encoder protocol.MessageEncoder) *Builder {
	b.messageEncoder = encoder
	return b
}

func (b *Builder) WithLightMode(light bool) *Builder {
	b.lightMode = light
	return b
}

func (b *Builder) WithConfirmedReads(confirmed bool) *Builder {
	b.confirmedReads = &confirmed
	return b
}

func (b *Builder) WithUseWebsocketToken(enabled bool) *Builder {
	b.useWebsocketToken = enabled
	return b
}

// WithUseWebSocketToken is an alias for WithUseWebsocketToken using idiomatic Go acronym casing.
func (b *Builder) WithUseWebSocketToken(enabled bool) *Builder {
	return b.WithUseWebsocketToken(enabled)
}

func (b *Builder) OnConnect(cb func(*Connection)) *Builder {
	b.onConnect = cb
	return b
}

func (b *Builder) OnConnectError(cb func(error)) *Builder {
	b.onConnectError = cb
	return b
}

func (b *Builder) OnDisconnect(cb func(error)) *Builder {
	b.onDisconnect = cb
	return b
}

func (b *Builder) OnMessage(cb func([]byte)) *Builder {
	b.onMessage = cb
	return b
}

func (b *Builder) Build(ctx context.Context) (*Connection, error) {
	if b.uri == "" {
		return nil, errors.New("uri is required")
	}
	if b.databaseName == "" {
		return nil, errors.New("database name is required")
	}
	if b.compression != protocol.CompressionGzip && b.compression != protocol.CompressionNone {
		return nil, fmt.Errorf("invalid compression: %q", b.compression)
	}

	hostURL, err := normalizeHostURL(b.uri)
	if err != nil {
		return nil, err
	}

	connectionID, err := randomConnectionID()
	if err != nil {
		return nil, fmt.Errorf("create connection id: %w", err)
	}

	wsURL := buildSubscribeURL(hostURL, b.databaseName, connectionID, b.compression, b.lightMode, b.confirmedReads)
	headers := map[string]string{}

	if b.token != "" {
		if b.useWebsocketToken {
			websocketToken, err := exchangeWebsocketToken(ctx, hostURL, b.token)
			if err != nil {
				if b.onConnectError != nil {
					b.onConnectError(err)
				}
				return nil, err
			}
			q := wsURL.Query()
			q.Set("token", websocketToken)
			wsURL.RawQuery = q.Encode()
		} else {
			headers["Authorization"] = "Bearer " + b.token
		}
	}

	conn, err := dialWebsocket(ctx, wsURL, headers)
	if err != nil {
		if b.onConnectError != nil {
			b.onConnectError(err)
		}
		return nil, err
	}

	c := newConnection(conn, connectionID, wsURL.String(), b.messageDecoder, b.messageEncoder, b.onMessage, b.onDisconnect)
	if b.onConnect != nil {
		b.onConnect(c)
	}
	c.startReadLoop()

	return c, nil
}

func normalizeHostURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse uri: %w", err)
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid uri %q: missing host", raw)
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u, nil
}

func randomConnectionID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func buildSubscribeURL(host *url.URL, databaseName, connectionID string, compression protocol.Compression, light bool, confirmed *bool) *url.URL {
	u := *host
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}
	u.Path = fmt.Sprintf("/v1/database/%s/subscribe", databaseName)

	q := u.Query()
	q.Set("connection_id", connectionID)
	q.Set("compression", string(compression))
	if light {
		q.Set("light", "true")
	}
	if confirmed != nil {
		if *confirmed {
			q.Set("confirmed", "true")
		} else {
			q.Set("confirmed", "false")
		}
	}
	u.RawQuery = q.Encode()
	return &u
}
