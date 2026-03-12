//go:build tinygo

package spacetimedb

import (
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// ── Reducer handler registry ─────────────────────────────────────────────────

// ReducerHandler is a function that implements a reducer body.
// It receives the caller context and a BytesSource handle from which to read
// the BSATN-encoded arguments.
type ReducerHandler func(ctx ReducerContext, args sys.BytesSource)

// reducerHandlers is populated by generated init() code via RegisterReducerHandler.
// The index of each entry corresponds to the reducer ID assigned by the module
// descriptor (i.e., the position in reducerRegistry at __describe_module__ time).
var reducerHandlers []ReducerHandler

// RegisterReducerHandler appends a reducer handler to the dispatch table.
// Generated bindings call this from package-level init() functions in the same
// order that they call RegisterReducerDef, so handler[i] corresponds to def[i].
func RegisterReducerHandler(fn ReducerHandler) {
	reducerHandlers = append(reducerHandlers, fn)
}

// ── WASM export ───────────────────────────────────────────────────────────────

// __call_reducer__ is invoked by the SpacetimeDB host to execute a reducer.
//
// Parameters:
//   - id:             index into the reducers slice from __describe_module__
//   - sender_0..3:    caller's Identity as 4 little-endian u64s (32 bytes total)
//   - conn_id_0..1:   caller's ConnectionId as 2 little-endian u64s (16 bytes); zero if no connection
//   - timestamp:      call time in microseconds since the Unix epoch
//   - args:           BytesSource handle containing BSATN-encoded reducer arguments
//   - errSink:        BytesSink handle for writing an error message on failure
//
// Return values:
//   - 0:  OK
//   - -1: no such reducer (id out of range)
//   - 1:  host call failure (reducer panicked or returned an error)
//
//export __call_reducer__
func callReducer(
	id uint32,
	sender0, sender1, sender2, sender3 uint64,
	connID0, connID1 uint64,
	timestamp uint64,
	args sys.BytesSource,
	errSink sys.BytesSink,
) int32 {
	if int(id) >= len(reducerHandlers) {
		msg := "no such reducer"
		_ = sys.WriteBytesToSink(errSink, []byte(msg))
		return -1
	}

	// Build the 32-byte Identity from four u64 parts (little-endian byte order).
	var senderBytes [32]byte
	binary.LittleEndian.PutUint64(senderBytes[0:8], sender0)
	binary.LittleEndian.PutUint64(senderBytes[8:16], sender1)
	binary.LittleEndian.PutUint64(senderBytes[16:24], sender2)
	binary.LittleEndian.PutUint64(senderBytes[24:32], sender3)
	sender := types.Identity(senderBytes)

	// Build the optional 16-byte ConnectionId from two u64 parts.
	var connID *types.ConnectionId
	if connID0 != 0 || connID1 != 0 {
		var connBytes [16]byte
		binary.LittleEndian.PutUint64(connBytes[0:8], connID0)
		binary.LittleEndian.PutUint64(connBytes[8:16], connID1)
		c := types.ConnectionId(connBytes)
		connID = &c
	}

	ctx := ReducerContext{
		Sender:       sender,
		ConnectionId: connID,
		Timestamp:    types.Timestamp{Microseconds: int64(timestamp)},
		Rng:          rand.New(rand.NewSource(int64(timestamp))),
		Auth:         newAuthCtxFromConnection(connID, sender),
	}

	// Execute the reducer with panic recovery.
	// On panic, write the error message to errSink and return HOST_CALL_FAILURE (1).
	var result int32
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("%v", r)
				_ = sys.WriteBytesToSink(errSink, []byte(msg))
				result = 1
			}
		}()
		reducerHandlers[id](ctx, args)
	}()
	return result
}
