package events

import "github.com/clockworklabs/spacetimedb-go/internal/protocol"

// ResultCallback is the shared callback signature for routed result events.
type ResultCallback func(protocol.RoutedMessage, error)

type ReducerResultCallback = ResultCallback
type ProcedureResultCallback = ResultCallback
type OneOffQueryResultCallback = ResultCallback
