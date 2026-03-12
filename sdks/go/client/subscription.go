package client

import (
	"sync"
	"sync/atomic"
)

// SubscriptionState tracks the lifecycle of a subscription.
type SubscriptionState int32

const (
	SubscriptionStatePending SubscriptionState = iota
	SubscriptionStateSent
	SubscriptionStateApplied
	SubscriptionStateEnded
	SubscriptionStateError
)

// SubscriptionHandle represents an active subscription to a query set.
type SubscriptionHandle struct {
	querySetId uint32
	state      atomic.Int32
	mu         sync.Mutex
	onEnded    func()
}

func newSubscriptionHandle(querySetId uint32) *SubscriptionHandle {
	h := &SubscriptionHandle{querySetId: querySetId}
	h.state.Store(int32(SubscriptionStateSent))
	return h
}

// QuerySetId returns the query set identifier for this subscription.
func (h *SubscriptionHandle) QuerySetId() uint32 { return h.querySetId }

// IsActive reports whether the subscription is in the Applied state.
func (h *SubscriptionHandle) IsActive() bool {
	return SubscriptionState(h.state.Load()) == SubscriptionStateApplied
}

// IsEnded reports whether the subscription has ended.
func (h *SubscriptionHandle) IsEnded() bool {
	s := SubscriptionState(h.state.Load())
	return s == SubscriptionStateEnded || s == SubscriptionStateError
}

// Unsubscribe ends the subscription. The caller is responsible for sending
// the Unsubscribe protocol message via DbConnection.
func (h *SubscriptionHandle) Unsubscribe() {
	h.state.Store(int32(SubscriptionStateEnded))
	h.mu.Lock()
	cb := h.onEnded
	h.onEnded = nil
	h.mu.Unlock()
	if cb != nil {
		cb()
	}
}

// UnsubscribeThen sets a callback to be called when the subscription ends,
// then ends the subscription.
func (h *SubscriptionHandle) UnsubscribeThen(onEnded func()) {
	h.mu.Lock()
	h.onEnded = onEnded
	h.mu.Unlock()
	h.Unsubscribe()
}

func (h *SubscriptionHandle) markApplied() {
	h.state.Store(int32(SubscriptionStateApplied))
}

func (h *SubscriptionHandle) markError() {
	h.state.Store(int32(SubscriptionStateError))
}

// SubscriptionBuilder is a fluent builder for creating subscriptions.
type SubscriptionBuilder struct {
	conn      *DbConnection
	onApplied func(querySetId uint32)
	onError   func(querySetId uint32, err string)
}

// NewSubscriptionBuilder returns a builder attached to conn.
func NewSubscriptionBuilder(conn *DbConnection) *SubscriptionBuilder {
	return &SubscriptionBuilder{conn: conn}
}

// OnApplied registers a callback fired when the server acknowledges the subscription.
func (b *SubscriptionBuilder) OnApplied(fn func(querySetId uint32)) *SubscriptionBuilder {
	b.onApplied = fn
	return b
}

// OnError registers a callback fired if the subscription fails.
func (b *SubscriptionBuilder) OnError(fn func(querySetId uint32, err string)) *SubscriptionBuilder {
	b.onError = fn
	return b
}

// Subscribe sends the subscription request and returns the handle.
func (b *SubscriptionBuilder) Subscribe(queries []string) (*SubscriptionHandle, error) {
	qsid, err := b.conn.Subscribe(queries)
	if err != nil {
		return nil, err
	}
	h := newSubscriptionHandle(qsid)
	b.conn.subscriptionManager.register(h, b.onApplied, b.onError)
	return h, nil
}

// SubscribeToAllTables subscribes to all tables in the module.
func (b *SubscriptionBuilder) SubscribeToAllTables() (*SubscriptionHandle, error) {
	return b.Subscribe([]string{"SELECT * FROM *"})
}

// subscriptionManager tracks active subscriptions and routes server responses.
type subscriptionManager struct {
	mu          sync.Mutex
	handles     map[uint32]*SubscriptionHandle
	onApplied   map[uint32]func(uint32)
	onError     map[uint32]func(uint32, string)
}

func newSubscriptionManager() *subscriptionManager {
	return &subscriptionManager{
		handles:   make(map[uint32]*SubscriptionHandle),
		onApplied: make(map[uint32]func(uint32)),
		onError:   make(map[uint32]func(uint32, string)),
	}
}

func (m *subscriptionManager) register(h *SubscriptionHandle, onApplied func(uint32), onError func(uint32, string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handles[h.querySetId] = h
	if onApplied != nil {
		m.onApplied[h.querySetId] = onApplied
	}
	if onError != nil {
		m.onError[h.querySetId] = onError
	}
}

func (m *subscriptionManager) handleApplied(querySetId uint32) {
	m.mu.Lock()
	h := m.handles[querySetId]
	cb := m.onApplied[querySetId]
	m.mu.Unlock()

	if h != nil {
		h.markApplied()
	}
	if cb != nil {
		cb(querySetId)
	}
}

func (m *subscriptionManager) handleError(querySetId uint32, errMsg string) {
	m.mu.Lock()
	h := m.handles[querySetId]
	cb := m.onError[querySetId]
	m.mu.Unlock()

	if h != nil {
		h.markError()
	}
	if cb != nil {
		cb(querySetId, errMsg)
	}
}

func (m *subscriptionManager) handleUnsubscribeApplied(querySetId uint32) {
	m.mu.Lock()
	h := m.handles[querySetId]
	delete(m.handles, querySetId)
	delete(m.onApplied, querySetId)
	delete(m.onError, querySetId)
	m.mu.Unlock()

	if h != nil {
		h.Unsubscribe()
	}
}
