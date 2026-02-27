package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	spacetimedb "github.com/clockworklabs/spacetimedb/sdks/go"
)

func main() {
	host := getenv("SPACETIMEDB_HOST", "http://localhost:3000")
	dbName := getenv("SPACETIMEDB_DB_NAME", "my-db")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := spacetimedb.NewDbConnectionBuilder().
		WithURI(host).
		WithDatabaseName(dbName).
		OnConnect(func(_ *spacetimedb.DbConnection) {
			log.Printf("Connected to SpacetimeDB at %s (database=%s)", host, dbName)
		}).
		OnConnectError(func(err error) {
			log.Printf("Connect error: %v", err)
		}).
		OnDisconnect(func(_ *spacetimedb.DbConnection, err error) {
			log.Printf("Disconnected: %v", err)
		}).
		Build(ctx)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer func() {
		if err := conn.Disconnect(); err != nil {
			log.Printf("disconnect error: %v", err)
		}
	}()

	if _, err := conn.Subscribe(ctx, []string{"SELECT * FROM person"}, nil); err != nil {
		log.Printf("subscribe error: %v", err)
	}

	log.Println("Client is running. Press Ctrl+C to exit.")
	<-ctx.Done()
	log.Println("Shutting down")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}