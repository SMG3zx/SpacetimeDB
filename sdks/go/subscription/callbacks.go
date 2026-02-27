package subscription

import "github.com/SMG3zx/SpacetimeDB/sdks/go/internal/protocol"

// Callback receives routed subscription lifecycle/update messages.
type Callback func(protocol.RoutedMessage, error)

// IsExpectedMessageKind returns true for message kinds produced by a subscription route.
func IsExpectedMessageKind(kind protocol.MessageKind) bool {
	switch kind {
	case protocol.MessageKindSubscribeApplied,
		protocol.MessageKindTransactionUpdate,
		protocol.MessageKindSubscriptionError,
		protocol.MessageKindUnsubscribeApplied:
		return true
	default:
		return false
	}
}

// IsTerminalMessageKind returns true when a subscription route should be cleaned up.
func IsTerminalMessageKind(kind protocol.MessageKind) bool {
	switch kind {
	case protocol.MessageKindSubscriptionError, protocol.MessageKindUnsubscribeApplied:
		return true
	default:
		return false
	}
}
