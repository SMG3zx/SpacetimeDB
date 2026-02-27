package spacetimedb

import (
	"context"
	"errors"
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

	_, err := NewDbConnectionBuilder().
		WithURI("http://127.0.0.1:1").
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err := NewDbConnectionBuilder().
		WithURI("http://127.0.0.1:1").
		WithDatabaseName("db").
		WithConnectRetry(10, 250*time.Millisecond).
		Build(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
