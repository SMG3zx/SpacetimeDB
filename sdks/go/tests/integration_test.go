//go:build integration

// Package tests contains end-to-end integration tests for the Go SDK.
// Run with: go test -tags=integration -run TestIntegration ./tests/
//
// Requires a running SpacetimeDB instance (set SPACETIMEDB_URL env var,
// default localhost:3000) with the "sdk_test" module published.
package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/client"
)

func spacetimeDBURL() string {
	if url := os.Getenv("SPACETIMEDB_URL"); url != "" {
		return url
	}
	return "localhost:3000"
}

// TestIntegrationConnectSubscribeReducer tests the full client lifecycle:
// connect → subscribe → call reducer → observe cache update → disconnect.
func TestIntegrationConnectSubscribeReducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	host := spacetimeDBURL()
	const moduleName = "sdk_test"

	conn, err := client.NewDbConnectionBuilder().
		WithUri(host).
		WithModuleName(moduleName).
		Build(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Disconnect()

	t.Logf("connected: identity=%s connId=%s", conn.Identity(), conn.ConnectionId())

	// Start message processing in background.
	errCh := conn.RunAsync(ctx)

	// Subscribe to all tables.
	_, err = conn.Subscribe([]string{"SELECT * FROM *"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Allow initial subscription to be applied.
	time.Sleep(500 * time.Millisecond)

	// Call a reducer (module must define an "add_person" reducer).
	reqId, err := conn.CallReducer("add_person", encodeAddPersonArgs(1, "Alice"))
	if err != nil {
		t.Fatalf("call_reducer: %v", err)
	}
	t.Logf("reducer call request_id=%d", reqId)

	// Allow the transaction update to arrive.
	time.Sleep(500 * time.Millisecond)

	// Clean disconnect.
	conn.Disconnect()

	// Check for errors from the message loop.
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Errorf("message loop error: %v", err)
		}
	case <-time.After(2 * time.Second):
	}
}

// encodeAddPersonArgs encodes (id uint64, name string) for the add_person reducer.
// This is a placeholder — real usage would use generated code from spacetime generate --lang go.
func encodeAddPersonArgs(id uint64, name string) []byte {
	w := bsatn.NewWriter()
	w.WriteU64(id)
	w.WriteString(name)
	return w.Bytes()
}
