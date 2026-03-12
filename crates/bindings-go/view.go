//go:build tinygo

package spacetimedb

import (
	"encoding/binary"
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// viewResultHeaderRowData is the pre-encoded BSATN for ViewResultHeader::RowData
// (variant tag 0, unit payload). Avoids per-call allocation.
var viewResultHeaderRowData = []byte{0}

// writeViewResultHeaderRowData writes the ViewResultHeader::RowData variant
// to the BytesSink. This is a tagged enum with tag 0 and Unit payload.
// Must be written before the actual row data in __call_view__ / __call_view_anon__.
func writeViewResultHeaderRowData(sink sys.BytesSink) {
	_ = sys.WriteBytesToSink(sink, viewResultHeaderRowData)
}

// ── View types ────────────────────────────────────────────────────────────────

// ViewHandler implements an authenticated view body.
// The handler reads args and writes BSATN-encoded rows to rows.
type ViewHandler func(
	sender types.Identity,
	connectionId *types.ConnectionId,
	args sys.BytesSource,
	rows sys.BytesSink,
)

// ViewAnonHandler implements an anonymous view body (no caller identity).
type ViewAnonHandler func(args sys.BytesSource, rows sys.BytesSink)

// ViewDef describes a view to be registered in the module.
type ViewDef struct {
	// Name is the view name as exposed to clients.
	Name string
	// IsPublic controls whether this view is callable from clients.
	IsPublic bool
	// IsAnonymous is true when the view does not receive caller identity.
	IsAnonymous bool
	// Params describes the input parameter types and names.
	Params []ColumnDef
	// ReturnType is the AlgebraicType of the row type returned by the view.
	ReturnType interface{} // types.AlgebraicType
}

// viewRegistry holds view definitions populated by init() via RegisterViewDef.
// viewHandlers and viewAnonHandlers hold the corresponding handler functions,
// indexed in the same order as viewRegistry. Authenticated and anonymous views
// are dispatched through separate handler slices.
var (
	viewRegistry     []ViewDef
	viewHandlers     []ViewHandler
	viewAnonHandlers []ViewAnonHandler
)

// RegisterViewDef adds a view descriptor to the module registry.
func RegisterViewDef(def ViewDef) {
	viewRegistry = append(viewRegistry, def)
}

// RegisterViewHandler appends an authenticated view handler to the dispatch table.
// Must be called in the same order as RegisterViewDef for non-anonymous views.
func RegisterViewHandler(fn ViewHandler) {
	viewHandlers = append(viewHandlers, fn)
}

// RegisterViewAnonHandler appends an anonymous view handler to the dispatch table.
// Must be called in the same order as RegisterViewDef for anonymous views.
func RegisterViewAnonHandler(fn ViewAnonHandler) {
	viewAnonHandlers = append(viewAnonHandlers, fn)
}

// ── WASM exports ──────────────────────────────────────────────────────────────

// __call_view__ is invoked by the SpacetimeDB host to execute an authenticated view.
//
//export __call_view__
func callView(
	id uint32,
	sender0, sender1, sender2, sender3 uint64,
	args sys.BytesSource,
	rows sys.BytesSink,
) int16 {
	if int(id) >= len(viewHandlers) {
		return -1
	}

	var senderBytes [32]byte
	binary.LittleEndian.PutUint64(senderBytes[0:8], sender0)
	binary.LittleEndian.PutUint64(senderBytes[8:16], sender1)
	binary.LittleEndian.PutUint64(senderBytes[16:24], sender2)
	binary.LittleEndian.PutUint64(senderBytes[24:32], sender3)
	sender := types.Identity(senderBytes)

	writeViewResultHeaderRowData(rows)
	var result int16 = 2 // ABI version identifier
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("%v", r)
				_ = sys.WriteBytesToSink(rows, []byte(msg))
				result = 1
			}
		}()
		viewHandlers[id](sender, nil, args, rows)
	}()
	return result
}

// __call_view_anon__ is invoked by the SpacetimeDB host to execute an anonymous view.
//
//export __call_view_anon__
func callViewAnon(
	id uint32,
	args sys.BytesSource,
	rows sys.BytesSink,
) int16 {
	if int(id) >= len(viewAnonHandlers) {
		return -1
	}
	writeViewResultHeaderRowData(rows)
	var result int16 = 2 // ABI version identifier
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("%v", r)
				_ = sys.WriteBytesToSink(rows, []byte(msg))
				result = 1
			}
		}()
		viewAnonHandlers[id](args, rows)
	}()
	return result
}

// ── View section in module def ────────────────────────────────────────────────

// writeViewDef serializes a RawViewDefV10 value.
func writeViewDef(w *bsatn.Writer, v ViewDef, index uint32) {
	// source_name: RawIdentifier
	w.WriteString(v.Name)
	// index: u32
	w.WriteU32(index)
	// is_public: bool
	w.WriteBool(v.IsPublic)
	// is_anonymous: bool
	w.WriteBool(v.IsAnonymous)
	// params: ProductType (inline, not in typespace)
	w.WriteArrayLen(uint32(len(v.Params)))
	for _, p := range v.Params {
		name := p.Name
		writeOptString(w, &name)
		types.WriteAlgebraicType(w, p.Type)
	}
	// return_type: AlgebraicType
	if at, ok := v.ReturnType.(types.AlgebraicType); ok {
		types.WriteAlgebraicType(w, at)
	} else {
		types.WriteAlgebraicType(w, types.ProductType{})
	}
}
