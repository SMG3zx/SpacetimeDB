package connection

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb/sdks/go/events"
	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
	sdksubscription "github.com/clockworklabs/spacetimedb/sdks/go/subscription"
)

type OneOffQueryResultCallback = events.OneOffQueryResultCallback
type SubscriptionCallback = sdksubscription.Callback

type subscriptionCallback = sdksubscription.Callback

func (c *Connection) OneOffQuery(query string, callback OneOffQueryResultCallback) (uint32, error) {
	if query == "" {
		return 0, fmt.Errorf("query is required")
	}

	return c.callWithRequestRoute(
		protocol.ClientMessage{
			Kind:      protocol.ClientMessageOneOffQuery,
			RequestID: c.NextRequestID(),
			Query:     query,
		},
		protocol.MessageKindOneOffQueryResult,
		callResultCallback(callback),
	)
}

func (c *Connection) Subscribe(queryStrings []string, callback SubscriptionCallback) (uint32, error) {
	if len(queryStrings) == 0 {
		return 0, fmt.Errorf("at least one query string is required")
	}
	for _, query := range queryStrings {
		if query == "" {
			return 0, fmt.Errorf("query strings must be non-empty")
		}
	}

	queryID := c.NextQueryID()
	requestID := c.NextRequestID()
	if callback != nil {
		wrapped := subscriptionCallback(func(message protocol.RoutedMessage, err error) {
			callback(message, err)
		})
		c.subCallbacks.Store(queryID, wrapped)
		c.OnQuery(queryID, func(message protocol.RoutedMessage) {
			if !sdksubscription.IsExpectedMessageKind(message.Kind) {
				callback(message, fmt.Errorf("unexpected subscription message kind: %q", message.Kind))
				return
			}

			if sdksubscription.IsTerminalMessageKind(message.Kind) {
				c.subCallbacks.Delete(queryID)
				c.ClearQueryRoute(queryID)
			}
			callback(message, nil)
		})
	}

	if err := c.sendClientMessage(protocol.ClientMessage{
		Kind:         protocol.ClientMessageSubscribe,
		RequestID:    requestID,
		QueryID:      &queryID,
		QueryStrings: queryStrings,
	}); err != nil {
		if callback != nil {
			c.subCallbacks.Delete(queryID)
			c.ClearQueryRoute(queryID)
		}
		return queryID, err
	}

	return queryID, nil
}

func (c *Connection) Unsubscribe(queryID uint32) (uint32, error) {
	requestID := c.NextRequestID()
	if err := c.sendClientMessage(protocol.ClientMessage{
		Kind:      protocol.ClientMessageUnsubscribe,
		RequestID: requestID,
		QueryID:   &queryID,
	}); err != nil {
		return requestID, err
	}
	return requestID, nil
}
