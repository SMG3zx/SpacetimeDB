//go:build tinygo

// Package spacetimedb provides the SpacetimeDB server-side Go SDK.
// It is compiled to WebAssembly using TinyGo and linked against the SpacetimeDB host ABI.
//
// Compile a module with:
//
//	tinygo build -target wasm-unknown -buildmode=c-shared -o module.wasm ./
package spacetimedb

import (
	"math/rand"
	"runtime"

	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ReducerContext is passed to every reducer function.
// It provides the caller's identity, optional connection ID, the call timestamp,
// and a deterministic random number generator seeded from the timestamp.
// Table accessors are provided by the generated module bindings via package-level variables.
type ReducerContext struct {
	// Sender is the identity of the client or scheduled-reducer that called this reducer.
	Sender types.Identity

	// ConnectionId is the connection ID of the caller, or nil for scheduled reducers.
	ConnectionId *types.ConnectionId

	// Timestamp is the time at which this reducer was invoked.
	Timestamp types.Timestamp

	// Rng is a deterministic pseudo-random generator seeded from the call timestamp.
	// All reducers with the same timestamp produce the same random sequence,
	// ensuring determinism across replicas.
	Rng *rand.Rand

	// Auth provides access to the JWT claims for the current call.
	// Use Auth.GetJwt() to load claims lazily. IsInternal is true for scheduled reducers.
	Auth AuthCtx
}

// LogLevel controls the severity of a log message.
type LogLevel uint32

const (
	LogLevelError LogLevel = 0
	LogLevelWarn  LogLevel = 1
	LogLevelInfo  LogLevel = 2
	LogLevelDebug LogLevel = 3
	LogLevelTrace LogLevel = 4
	LogLevelPanic LogLevel = 101
)

// logWithCaller writes msg at the given level, capturing the call site of the
// caller `skip` frames above this function (skip=2 reaches the public API caller).
func logWithCaller(skip int, level LogLevel, msg string) {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file, line = "", 0
	}
	// Use only the base filename, not the full path.
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' || file[i] == '\\' {
			short = file[i+1:]
			break
		}
	}
	sys.ConsoleLog(uint32(level), "", short, uint32(line), msg)
}

// Log writes msg at the given level to the SpacetimeDB host log.
// The caller's file and line number are captured automatically.
func Log(level LogLevel, msg string) { logWithCaller(2, level, msg) }

// LogError writes msg at Error level.
func LogError(msg string) { logWithCaller(2, LogLevelError, msg) }

// LogWarn writes msg at Warn level.
func LogWarn(msg string) { logWithCaller(2, LogLevelWarn, msg) }

// LogInfo writes msg at Info level.
func LogInfo(msg string) { logWithCaller(2, LogLevelInfo, msg) }

// LogDebug writes msg at Debug level.
func LogDebug(msg string) { logWithCaller(2, LogLevelDebug, msg) }

// LogTrace writes msg at Trace level.
func LogTrace(msg string) { logWithCaller(2, LogLevelTrace, msg) }

// LogPanic writes msg at Panic level then panics.
// The panic will cause the host to roll back the current transaction.
func LogPanic(msg string) {
	logWithCaller(2, LogLevelPanic, msg)
	panic(msg)
}

// ── LogStopwatch ──────────────────────────────────────────────────────────────

// LogStopwatch wraps a host timing span with an idiomatic Go interface.
// Create one with NewLogStopwatch and defer Stop to guarantee cleanup:
//
//	sw := spacetimedb.NewLogStopwatch("myop")
//	defer sw.Stop()
type LogStopwatch struct {
	id uint32
}

// NewLogStopwatch starts a new timing span named `name` on the SpacetimeDB host
// and returns a LogStopwatch that can be stopped with Stop().
func NewLogStopwatch(name string) LogStopwatch {
	return LogStopwatch{id: sys.ConsoleTimerStart(name)}
}

// Stop ends the timing span and logs its elapsed duration to the host.
func (sw LogStopwatch) Stop() {
	_ = sys.ConsoleTimerEnd(sw.id)
}

// ── Module-level helpers ──────────────────────────────────────────────────────

// ModuleIdentity returns the 32-byte identity of this module on the host.
func ModuleIdentity() types.Identity {
	return types.Identity(sys.Identity())
}

// VolatileNonatomicScheduleImmediate schedules a reducer call by name outside
// the current transaction. The call is not guaranteed to be atomic with the
// current transaction. args is the BSATN-encoded argument list.
func VolatileNonatomicScheduleImmediate(name string, args []byte) {
	sys.VolatileNonatomicScheduleImmediate(name, args)
}

// ── Sentinel errors ───────────────────────────────────────────────────────────
//
// These re-export the sys.Errno constants at the spacetimedb package level
// so callers can check specific errors without importing the sys package.
// Use errors.Is() for comparisons:
//
//	if errors.Is(err, spacetimedb.ErrNoSuchTable) { ... }
var (
	ErrHostCallFailure         = sys.ErrHostCallFailure
	ErrNotInTransaction        = sys.ErrNotInTransaction
	ErrBsatnDecodeError        = sys.ErrBsatnDecodeError
	ErrNoSuchTable             = sys.ErrNoSuchTable
	ErrNoSuchIndex             = sys.ErrNoSuchIndex
	ErrNoSuchIter              = sys.ErrNoSuchIter
	ErrNoSuchConsoleTimer      = sys.ErrNoSuchConsoleTimer
	ErrNoSuchBytes             = sys.ErrNoSuchBytes
	ErrNoSpace                 = sys.ErrNoSpace
	ErrWrongIndexAlgo          = sys.ErrWrongIndexAlgo
	ErrBufferTooSmall          = sys.ErrBufferTooSmall
	ErrUniqueAlreadyExists     = sys.ErrUniqueAlreadyExists
	ErrScheduleAtDelayTooLong  = sys.ErrScheduleAtDelayTooLong
	ErrIndexNotUnique          = sys.ErrIndexNotUnique
	ErrNoSuchRow               = sys.ErrNoSuchRow
	ErrAutoIncOverflow         = sys.ErrAutoIncOverflow
	ErrWouldBlockTransaction   = sys.ErrWouldBlockTransaction
	ErrTransactionNotAnonymous = sys.ErrTransactionNotAnonymous
	ErrTransactionIsReadOnly   = sys.ErrTransactionIsReadOnly
	ErrTransactionIsMut        = sys.ErrTransactionIsMut
	ErrHttpError               = sys.ErrHttpError
)
