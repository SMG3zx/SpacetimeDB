package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/clockworklabs/spacetimedb-go/protocol"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// QueryResult is the result of a one-off SQL query.
type QueryResult struct {
	Tables []QueryResultTable
}

// QueryResultTable holds the rows for a single table returned by a one-off query.
type QueryResultTable struct {
	TableName string
	Rows      [][]byte // raw BSATN-encoded rows
}

// DbConnection is an active connection to a SpacetimeDB database.
// Use DbConnectionBuilder to create one.
type DbConnection struct {
	ws           *WsConnection
	identity     types.Identity
	connectionId types.ConnectionId
	token        string
	requestId    atomic.Uint32
	active       atomic.Bool
	onDisconnect func(err error)

	// tableHandlers maps table name -> handler that applies row updates.
	tableHandlers       map[string]TableUpdateHandler
	subscriptionManager *subscriptionManager

	mu                 sync.Mutex
	pendingOneOff      map[uint32]chan *protocol.OneOffQueryResultMsg
	pendingProcedures  map[uint32]chan *protocol.ProcedureResultMsg
}

// TableUpdateHandler processes row inserts and deletes for a specific table.
type TableUpdateHandler interface {
	ApplyInserts(rows *protocol.BsatnRowList) error
	ApplyDeletes(rows *protocol.BsatnRowList) error
}

func newDbConnection(
	ws *WsConnection,
	identity types.Identity,
	connectionId types.ConnectionId,
	token string,
	onDisconnect func(err error),
) *DbConnection {
	c := &DbConnection{
		ws:                  ws,
		identity:            identity,
		connectionId:        connectionId,
		token:               token,
		onDisconnect:        onDisconnect,
		tableHandlers:       make(map[string]TableUpdateHandler),
		subscriptionManager: newSubscriptionManager(),
		pendingOneOff:       make(map[uint32]chan *protocol.OneOffQueryResultMsg),
		pendingProcedures:   make(map[uint32]chan *protocol.ProcedureResultMsg),
	}
	c.active.Store(true)
	return c
}

// Identity returns the client's identity.
func (c *DbConnection) Identity() types.Identity { return c.identity }

// ConnectionId returns the connection identifier.
func (c *DbConnection) ConnectionId() types.ConnectionId { return c.connectionId }

// Token returns the authentication token received from the server.
func (c *DbConnection) Token() string { return c.token }

// IsActive reports whether the connection is open.
func (c *DbConnection) IsActive() bool { return c.active.Load() }

// Disconnect closes the WebSocket connection.
func (c *DbConnection) Disconnect() error {
	c.active.Store(false)
	return c.ws.Close()
}

// NextRequestId returns a monotonically increasing request ID.
func (c *DbConnection) NextRequestId() uint32 {
	return c.requestId.Add(1)
}

// RegisterTableHandler registers a handler for a named table's row updates.
func (c *DbConnection) RegisterTableHandler(tableName string, h TableUpdateHandler) {
	c.tableHandlers[tableName] = h
}

// CallReducer sends a CallReducer message and returns the request_id.
func (c *DbConnection) CallReducer(reducer string, argsBsatn []byte) (uint32, error) {
	reqId := c.NextRequestId()
	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageCallReducer,
		CallReducer: &protocol.CallReducerMsg{
			RequestId: reqId,
			Flags:     0,
			Reducer:   reducer,
			Args:      argsBsatn,
		},
	}
	return reqId, c.ws.Send(msg)
}

// Subscribe sends a Subscribe message and returns the query_set_id.
func (c *DbConnection) Subscribe(queries []string) (uint32, error) {
	reqId := c.NextRequestId()
	qsid := c.NextRequestId()
	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageSubscribe,
		Subscribe: &protocol.SubscribeMsg{
			RequestId:    reqId,
			QuerySetId:   protocol.QuerySetId{ID: qsid},
			QueryStrings: queries,
		},
	}
	return qsid, c.ws.Send(msg)
}

// AdvanceOneMessage processes the next incoming message. Blocks until a message
// arrives or ctx is cancelled.
func (c *DbConnection) AdvanceOneMessage(ctx context.Context) error {
	select {
	case pm, ok := <-c.ws.Recv():
		if !ok {
			c.active.Store(false)
			if c.onDisconnect != nil {
				c.onDisconnect(nil)
			}
			return nil
		}
		if pm.Err != nil {
			c.active.Store(false)
			if c.onDisconnect != nil {
				c.onDisconnect(pm.Err)
			}
			return pm.Err
		}
		return c.handleMessage(pm.Message)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FrameTick processes all currently available messages without blocking.
func (c *DbConnection) FrameTick() error {
	for {
		select {
		case pm, ok := <-c.ws.Recv():
			if !ok {
				c.active.Store(false)
				return nil
			}
			if pm.Err != nil {
				return pm.Err
			}
			if err := c.handleMessage(pm.Message); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// RunBlocking runs the message loop until the connection closes or ctx is cancelled.
func (c *DbConnection) RunBlocking(ctx context.Context) error {
	for c.IsActive() {
		if err := c.AdvanceOneMessage(ctx); err != nil {
			return err
		}
	}
	return nil
}

// RunAsync starts the message loop in a background goroutine.
// The returned channel receives the terminal error (nil on clean close).
func (c *DbConnection) RunAsync(ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- c.RunBlocking(ctx)
	}()
	return ch
}

// OneOffQuery executes a SQL query and returns the results without creating a subscription.
// The context can be used for cancellation / timeout.
func (c *DbConnection) OneOffQuery(ctx context.Context, query string) (*QueryResult, error) {
	reqId := c.NextRequestId()
	ch := make(chan *protocol.OneOffQueryResultMsg, 1)

	c.mu.Lock()
	c.pendingOneOff[reqId] = ch
	c.mu.Unlock()

	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageOneOffQuery,
		OneOffQuery: &protocol.OneOffQueryMsg{
			RequestId:   reqId,
			QueryString: query,
		},
	}
	if err := c.ws.Send(msg); err != nil {
		c.mu.Lock()
		delete(c.pendingOneOff, reqId)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case result := <-ch:
		if result.Err != "" {
			return nil, fmt.Errorf("one-off query: %s", result.Err)
		}
		qr := &QueryResult{}
		if result.Rows != nil {
			for _, t := range result.Rows.Tables {
				qt := QueryResultTable{TableName: t.Table}
				for row := range t.Rows.Rows() {
					qt.Rows = append(qt.Rows, row)
				}
				qr.Tables = append(qr.Tables, qt)
			}
		}
		return qr, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pendingOneOff, reqId)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// CallProcedure invokes a named procedure and returns its raw result bytes.
func (c *DbConnection) CallProcedure(ctx context.Context, procedureName string, args []byte) ([]byte, error) {
	reqId := c.NextRequestId()
	ch := make(chan *protocol.ProcedureResultMsg, 1)

	c.mu.Lock()
	c.pendingProcedures[reqId] = ch
	c.mu.Unlock()

	msg := protocol.ClientMessage{
		Kind: protocol.ClientMessageCallProcedure,
		CallProcedure: &protocol.CallProcedureMsg{
			RequestId: reqId,
			Flags:     0,
			Procedure: procedureName,
			Args:      args,
		},
	}
	if err := c.ws.Send(msg); err != nil {
		c.mu.Lock()
		delete(c.pendingProcedures, reqId)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case result := <-ch:
		switch result.Status.Kind {
		case protocol.ProcedureStatusReturned:
			return result.Status.ReturnValue, nil
		case protocol.ProcedureStatusInternalError:
			return nil, fmt.Errorf("procedure %q: internal error: %s", procedureName, result.Status.InternalError)
		default:
			return nil, fmt.Errorf("procedure %q: unknown status %d", procedureName, result.Status.Kind)
		}
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pendingProcedures, reqId)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}
