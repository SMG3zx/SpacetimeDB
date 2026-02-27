package connection

import (
	"context"
	"net/url"
	"testing"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
)

func TestNewBuilderDefaults(t *testing.T) {
	b := NewBuilder()
	if b.compression != protocol.CompressionGzip {
		t.Fatalf("unexpected default compression: %q", b.compression)
	}
	if b.messageDecoder == nil {
		t.Fatalf("expected default message decoder to be configured")
	}
	if !b.useWebsocketToken {
		t.Fatalf("expected websocket token exchange to be enabled by default")
	}
}

func TestBuildValidationErrors(t *testing.T) {
	ctx := context.Background()

	if _, err := NewBuilder().Build(ctx); err == nil {
		t.Fatalf("expected missing uri error")
	}

	if _, err := NewBuilder().WithURI("http://localhost:3000").Build(ctx); err == nil {
		t.Fatalf("expected missing database name error")
	}

	if _, err := NewBuilder().
		WithURI("http://localhost:3000").
		WithDatabaseName("db").
		WithCompression(protocol.Compression("invalid")).
		Build(ctx); err == nil {
		t.Fatalf("expected invalid compression error")
	}
}

func TestNormalizeHostURL(t *testing.T) {
	u, err := normalizeHostURL("//localhost:3000")
	if err != nil {
		t.Fatalf("normalize host: %v", err)
	}
	if u.Scheme != "http" {
		t.Fatalf("unexpected scheme: %q", u.Scheme)
	}
	if u.Host != "localhost:3000" {
		t.Fatalf("unexpected host: %q", u.Host)
	}
	if u.Path != "/" {
		t.Fatalf("unexpected path: %q", u.Path)
	}

	if _, err := normalizeHostURL("://bad"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestBuildSubscribeURL(t *testing.T) {
	host, err := url.Parse("https://example.com")
	if err != nil {
		t.Fatalf("parse host: %v", err)
	}
	confirmed := true

	u := buildSubscribeURL(host, "mydb", "conn-1", protocol.CompressionGzip, true, &confirmed)
	if u.Scheme != "wss" {
		t.Fatalf("unexpected scheme: %q", u.Scheme)
	}
	if u.Path != "/v1/database/mydb/subscribe" {
		t.Fatalf("unexpected path: %q", u.Path)
	}

	q := u.Query()
	if got := q.Get("connection_id"); got != "conn-1" {
		t.Fatalf("unexpected connection_id: %q", got)
	}
	if got := q.Get("compression"); got != string(protocol.CompressionGzip) {
		t.Fatalf("unexpected compression: %q", got)
	}
	if got := q.Get("light"); got != "true" {
		t.Fatalf("unexpected light: %q", got)
	}
	if got := q.Get("confirmed"); got != "true" {
		t.Fatalf("unexpected confirmed: %q", got)
	}
}

func TestWithMessageEncoderSetsEncoder(t *testing.T) {
	b := NewBuilder()
	encoder := func(message protocol.ClientMessage) ([]byte, error) {
		return []byte(message.Kind), nil
	}
	b.WithMessageEncoder(encoder)
	if b.messageEncoder == nil {
		t.Fatalf("expected message encoder to be set")
	}
}

func TestAliasMethods(t *testing.T) {
	b := NewBuilder()
	b.WithURL("http://localhost:3000")
	b.WithUseWebSocketToken(true)
	if b.uri != "http://localhost:3000" {
		t.Fatalf("expected WithURL alias to set uri")
	}
	if !b.useWebsocketToken {
		t.Fatalf("expected WithUseWebSocketToken alias to set useWebsocketToken")
	}
}
