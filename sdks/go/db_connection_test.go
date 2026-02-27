package spacetimedb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestDbConnectionContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	conn := &DbConnection{}

	if _, err := conn.CallReducer(ctx, "r", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from CallReducer, got: %v", err)
	}
	if _, err := conn.CallProcedure(ctx, "p", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from CallProcedure, got: %v", err)
	}
	if _, err := conn.OneOffQuery(ctx, "select 1", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from OneOffQuery, got: %v", err)
	}
	if _, err := conn.Subscribe(ctx, []string{"select 1"}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from Subscribe, got: %v", err)
	}
	if _, err := conn.Unsubscribe(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from Unsubscribe, got: %v", err)
	}
}

func TestDbConnectionBuilderConnectRetryAttempts(t *testing.T) {
	var connectErrors atomic.Int32
	port := lowestUnusedUnprivilegedPort(t)

	_, err := NewDbConnectionBuilder().
		WithURI(fmt.Sprintf("http://127.0.0.1:%d", port)).
		WithDatabaseName("db").
		WithConnectRetry(3, 0).
		OnConnectError(func(error) {
			connectErrors.Add(1)
		}).
		Build(context.Background())
	if err == nil {
		t.Fatalf("expected build to fail")
	}
	if got := connectErrors.Load(); got != 3 {
		t.Fatalf("expected 3 connect-error callbacks, got %d", got)
	}
}

func TestDbConnectionBuilderConnectRetryHonorsContextCancellation(t *testing.T) {
	port := lowestUnusedUnprivilegedPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err := NewDbConnectionBuilder().
		WithURI(fmt.Sprintf("http://127.0.0.1:%d", port)).
		WithDatabaseName("db").
		WithConnectRetry(10, 250*time.Millisecond).
		Build(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func lowestUnusedUnprivilegedPort(t *testing.T) int {
	t.Helper()
	for port := 1024; port <= 65535; port++ {
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = l.Close()
		return port
	}
	t.Fatal("failed to find an unused unprivileged localhost port")
	return 0
}
