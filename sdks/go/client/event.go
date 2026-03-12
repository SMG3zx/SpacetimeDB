package client

import (
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ReducerStatus describes the outcome of a reducer invocation.
type ReducerStatus int

const (
	ReducerStatusCommitted    ReducerStatus = iota // reducer committed successfully
	ReducerStatusFailed                            // reducer returned an error
	ReducerStatusOutOfEnergy                       // reducer ran out of energy
)

// ReducerEvent contains metadata about a reducer that triggered a transaction.
type ReducerEvent struct {
	Timestamp          types.Timestamp
	CallerIdentity     types.Identity
	CallerConnectionId types.ConnectionId
	ReducerName        string
	Status             ReducerStatus
	EnergyQuanta       int64
}

// EventContext is passed to row callbacks (OnInsert, OnDelete, OnUpdate).
// Db and Reducers are typed by codegen; the base types here use any.
type EventContext struct {
	Identity     types.Identity
	ConnectionId types.ConnectionId
	Db           any // typed as RemoteTables in codegen output
	Reducers     any // typed as RemoteReducers in codegen output
	Event        *ReducerEvent
}

// ReducerEventContext is passed to reducer callbacks.
type ReducerEventContext struct {
	EventContext
	ReducerName string
	Status      ReducerStatus
	Timestamp   types.Timestamp
}

// SubscriptionEventContext is passed to subscription lifecycle callbacks.
type SubscriptionEventContext struct {
	Identity     types.Identity
	ConnectionId types.ConnectionId
	Db           any
	Reducers     any
	QuerySetId   uint32
}

// ErrorContext is passed to error callbacks (OnDisconnect, OnConnectError).
type ErrorContext struct {
	Identity     *types.Identity     // nil before identity is established
	ConnectionId *types.ConnectionId // nil before connection is established
	Err          error
}
