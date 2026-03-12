package client

import (
	"sync"
	"sync/atomic"
)

// CallbackId uniquely identifies a registered callback.
type CallbackId uint64

var globalCallbackId atomic.Uint64

func nextCallbackId() CallbackId {
	return CallbackId(globalCallbackId.Add(1))
}

// CallbackRegistry holds a set of callbacks for a single event type.
// It is safe for concurrent use.
type CallbackRegistry[Ctx any, Arg any] struct {
	mu      sync.RWMutex
	entries map[CallbackId]func(Ctx, Arg)
}

// NewCallbackRegistry returns an initialised registry.
func NewCallbackRegistry[Ctx any, Arg any]() *CallbackRegistry[Ctx, Arg] {
	return &CallbackRegistry[Ctx, Arg]{
		entries: make(map[CallbackId]func(Ctx, Arg)),
	}
}

// Register adds fn to the registry and returns its id.
func (r *CallbackRegistry[Ctx, Arg]) Register(fn func(Ctx, Arg)) CallbackId {
	id := nextCallbackId()
	r.mu.Lock()
	r.entries[id] = fn
	r.mu.Unlock()
	return id
}

// Remove deletes the callback with the given id. Safe to call with an unknown id.
func (r *CallbackRegistry[Ctx, Arg]) Remove(id CallbackId) {
	r.mu.Lock()
	delete(r.entries, id)
	r.mu.Unlock()
}

// Invoke calls all registered callbacks with ctx and arg.
// Callbacks are invoked under a read lock; do not call Register/Remove from within a callback.
func (r *CallbackRegistry[Ctx, Arg]) Invoke(ctx Ctx, arg Arg) {
	r.mu.RLock()
	fns := make([]func(Ctx, Arg), 0, len(r.entries))
	for _, fn := range r.entries {
		fns = append(fns, fn)
	}
	r.mu.RUnlock()
	for _, fn := range fns {
		fn(ctx, arg)
	}
}

// Len returns the number of registered callbacks.
func (r *CallbackRegistry[Ctx, Arg]) Len() int {
	r.mu.RLock()
	n := len(r.entries)
	r.mu.RUnlock()
	return n
}

// TableCallbacks holds insert/delete/update callbacks for a single table.
type TableCallbacks[Ctx any, Row any] struct {
	OnInsert *CallbackRegistry[Ctx, Row]
	OnDelete *CallbackRegistry[Ctx, Row]
	OnUpdate *CallbackRegistry[Ctx, [2]Row] // [old, new]
}

// NewTableCallbacks returns an initialised TableCallbacks.
func NewTableCallbacks[Ctx any, Row any]() *TableCallbacks[Ctx, Row] {
	return &TableCallbacks[Ctx, Row]{
		OnInsert: NewCallbackRegistry[Ctx, Row](),
		OnDelete: NewCallbackRegistry[Ctx, Row](),
		OnUpdate: NewCallbackRegistry[Ctx, [2]Row](),
	}
}
