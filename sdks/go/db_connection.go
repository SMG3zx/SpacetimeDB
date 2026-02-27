package spacetimedb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SMG3zx/SpacetimeDB/sdks/go/connection"
	"github.com/SMG3zx/SpacetimeDB/sdks/go/internal/protocol"
)

type ReducerResultCallback = connection.ReducerResultCallback
type ProcedureResultCallback = connection.ProcedureResultCallback
type OneOffQueryResultCallback = connection.OneOffQueryResultCallback
type SubscriptionCallback = connection.SubscriptionCallback

type ConnectCallback func(*DbConnection)
type ConnectErrorCallback func(error)
type DisconnectCallback func(*DbConnection, error)
type MessageCallback func([]byte)
type ConnectInfoCallback func(*DbConnection, ConnectionInfo)

// ConnectionInfo captures identity/session metadata from initial_connection.
type ConnectionInfo struct {
	Identity     string
	ConnectionID string
	Token        string
	ReceivedAt   time.Time
}

// DbConnection is a high-level SDK connection facade over connection.Connection.
type DbConnection struct {
	conn *connection.Connection

	infoMu         sync.RWMutex
	connectionInfo *ConnectionInfo
}

func (c *DbConnection) Raw() *connection.Connection {
	if c == nil {
		return nil
	}
	return c.conn
}

func (c *DbConnection) IsActive() bool {
	return c != nil && c.conn != nil && c.conn.IsActive()
}

func (c *DbConnection) Disconnect() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Disconnect()
}

func (c *DbConnection) ConnectionInfo() (ConnectionInfo, bool) {
	if c == nil {
		return ConnectionInfo{}, false
	}
	c.infoMu.RLock()
	defer c.infoMu.RUnlock()
	if c.connectionInfo == nil {
		return ConnectionInfo{}, false
	}
	return *c.connectionInfo, true
}

func (c *DbConnection) Identity() (string, bool) {
	info, ok := c.ConnectionInfo()
	if !ok {
		return "", false
	}
	return info.Identity, true
}

func (c *DbConnection) InitialConnectionID() (string, bool) {
	info, ok := c.ConnectionInfo()
	if !ok {
		return "", false
	}
	return info.ConnectionID, true
}

func (c *DbConnection) CallReducer(ctx context.Context, reducer string, args []byte, callback ReducerResultCallback) (uint32, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if c == nil || c.conn == nil {
		return 0, notConnectedError("call_reducer")
	}
	return c.conn.CallReducer(reducer, args, callback)
}

func (c *DbConnection) CallProcedure(
	ctx context.Context,
	procedure string,
	args []byte,
	callback ProcedureResultCallback,
) (uint32, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if c == nil || c.conn == nil {
		return 0, notConnectedError("call_procedure")
	}
	return c.conn.CallProcedure(procedure, args, callback)
}

func (c *DbConnection) OneOffQuery(ctx context.Context, query string, callback OneOffQueryResultCallback) (uint32, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if c == nil || c.conn == nil {
		return 0, notConnectedError("one_off_query")
	}
	return c.conn.OneOffQuery(query, callback)
}

func (c *DbConnection) Subscribe(ctx context.Context, queryStrings []string, callback SubscriptionCallback) (uint32, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if c == nil || c.conn == nil {
		return 0, notConnectedError("subscribe")
	}
	return c.conn.Subscribe(queryStrings, callback)
}

func (c *DbConnection) Unsubscribe(ctx context.Context, queryID uint32) (uint32, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if c == nil || c.conn == nil {
		return 0, notConnectedError("unsubscribe")
	}
	return c.conn.Unsubscribe(queryID)
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func notConnectedError(op string) error {
	return &connection.Error{
		Code: connection.ErrorConnectionClosed,
		Op:   op,
		Err:  errors.New("spacetimedb client is not connected"),
	}
}

// DbConnectionBuilder is the high-level public builder.
type DbConnectionBuilder struct {
	inner *connection.Builder

	onConnect      ConnectCallback
	onConnectInfo  ConnectInfoCallback
	onConnectError ConnectErrorCallback
	onDisconnect   DisconnectCallback

	connectRetryMaxAttempts int
	connectRetryBackoff     time.Duration
}

func NewDbConnectionBuilder() *DbConnectionBuilder {
	return &DbConnectionBuilder{
		inner:                   connection.NewBuilder(),
		connectRetryMaxAttempts: 1,
	}
}

func (b *DbConnectionBuilder) WithURI(uri string) *DbConnectionBuilder {
	b.inner.WithURI(uri)
	return b
}

// WithURL is an alias for WithURI using idiomatic Go acronym casing.
func (b *DbConnectionBuilder) WithURL(uri string) *DbConnectionBuilder {
	return b.WithURI(uri)
}

func (b *DbConnectionBuilder) WithDatabaseName(name string) *DbConnectionBuilder {
	b.inner.WithDatabaseName(name)
	return b
}

func (b *DbConnectionBuilder) WithToken(token string) *DbConnectionBuilder {
	b.inner.WithToken(token)
	return b
}

func (b *DbConnectionBuilder) WithCompression(compression protocol.Compression) *DbConnectionBuilder {
	b.inner.WithCompression(compression)
	return b
}

func (b *DbConnectionBuilder) WithConfirmedReads(confirmed bool) *DbConnectionBuilder {
	b.inner.WithConfirmedReads(confirmed)
	return b
}

func (b *DbConnectionBuilder) WithUseWebsocketToken(enabled bool) *DbConnectionBuilder {
	b.inner.WithUseWebsocketToken(enabled)
	return b
}

// WithUseWebSocketToken is an alias for WithUseWebsocketToken using idiomatic Go acronym casing.
func (b *DbConnectionBuilder) WithUseWebSocketToken(enabled bool) *DbConnectionBuilder {
	return b.WithUseWebsocketToken(enabled)
}

func (b *DbConnectionBuilder) WithLightMode(light bool) *DbConnectionBuilder {
	b.inner.WithLightMode(light)
	return b
}

func (b *DbConnectionBuilder) WithMessageDecoder(decoder protocol.MessageDecoder) *DbConnectionBuilder {
	b.inner.WithMessageDecoder(decoder)
	return b
}

func (b *DbConnectionBuilder) WithMessageEncoder(encoder protocol.MessageEncoder) *DbConnectionBuilder {
	b.inner.WithMessageEncoder(encoder)
	return b
}

func (b *DbConnectionBuilder) OnConnect(cb ConnectCallback) *DbConnectionBuilder {
	b.onConnect = cb
	return b
}

func (b *DbConnectionBuilder) OnConnectInfo(cb ConnectInfoCallback) *DbConnectionBuilder {
	b.onConnectInfo = cb
	return b
}

func (b *DbConnectionBuilder) OnConnectError(cb ConnectErrorCallback) *DbConnectionBuilder {
	b.onConnectError = cb
	return b
}

func (b *DbConnectionBuilder) OnDisconnect(cb DisconnectCallback) *DbConnectionBuilder {
	b.onDisconnect = cb
	return b
}

func (b *DbConnectionBuilder) OnMessage(cb MessageCallback) *DbConnectionBuilder {
	b.inner.OnMessage(func(bytes []byte) {
		if cb != nil {
			cb(bytes)
		}
	})
	return b
}

// WithConnectRetry configures retries for initial Build connection attempts.
//
// maxAttempts includes the first attempt.
// - maxAttempts <= 0 is treated as 1.
// - backoff <= 0 performs retries without sleeping.
func (b *DbConnectionBuilder) WithConnectRetry(maxAttempts int, backoff time.Duration) *DbConnectionBuilder {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	b.connectRetryMaxAttempts = maxAttempts
	b.connectRetryBackoff = backoff
	return b
}

func (b *DbConnectionBuilder) Build(ctx context.Context) (*DbConnection, error) {
	var dbConn *DbConnection
	var onConnectInfoOnce sync.Once

	invokeConnectInfo := func(payload protocol.InitialConnectionPayload) {
		if dbConn == nil {
			return
		}
		info := ConnectionInfo{
			Identity:     payload.Identity,
			ConnectionID: payload.ConnectionID,
			Token:        payload.Token,
			ReceivedAt:   time.Now(),
		}
		dbConn.infoMu.Lock()
		dbConn.connectionInfo = &info
		dbConn.infoMu.Unlock()

		if b.onConnectInfo != nil {
			onConnectInfoOnce.Do(func() {
				b.onConnectInfo(dbConn, info)
			})
		}
	}

	b.inner.OnConnect(func(conn *connection.Connection) {
		dbConn = &DbConnection{conn: conn}
		conn.OnKind(protocol.MessageKindInitialConnection, func(message protocol.RoutedMessage) {
			payload, err := protocol.DecodeInitialConnectionPayload(message.Payload)
			if err != nil {
				if b.onConnectError != nil {
					b.onConnectError(err)
				}
				return
			}
			invokeConnectInfo(payload)
		})

		if b.onConnect != nil {
			b.onConnect(dbConn)
		}
	})
	b.inner.OnConnectError(func(err error) {
		if b.onConnectError != nil {
			b.onConnectError(err)
		}
	})
	b.inner.OnDisconnect(func(err error) {
		if b.onDisconnect != nil {
			b.onDisconnect(dbConn, err)
		}
	})

	attempts := b.connectRetryMaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	backoff := b.connectRetryBackoff

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := validateContext(ctx); err != nil {
			return nil, err
		}

		conn, err := b.inner.Build(ctx)
		if err == nil {
			if dbConn == nil {
				dbConn = &DbConnection{conn: conn}
			}
			return dbConn, nil
		}
		lastErr = err

		if attempt == attempts {
			break
		}
		if backoff <= 0 {
			continue
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, lastErr
}
