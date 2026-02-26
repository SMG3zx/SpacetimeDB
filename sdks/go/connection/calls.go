package connection

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
)

type ReducerResultCallback func(protocol.RoutedMessage, error)
type ProcedureResultCallback func(protocol.RoutedMessage, error)

type callResultCallback func(protocol.RoutedMessage, error)

func (c *Connection) CallReducer(reducer string, args []byte, callback ReducerResultCallback) (uint32, error) {
	if reducer == "" {
		return 0, fmt.Errorf("reducer name is required")
	}

	return c.callWithRequestRoute(
		protocol.ClientMessage{
			Kind:      protocol.ClientMessageCallReducer,
			RequestID: c.NextRequestID(),
			Reducer:   reducer,
			Args:      args,
		},
		protocol.MessageKindReducerResult,
		callResultCallback(callback),
	)
}

func (c *Connection) CallProcedure(procedure string, args []byte, callback ProcedureResultCallback) (uint32, error) {
	if procedure == "" {
		return 0, fmt.Errorf("procedure name is required")
	}

	return c.callWithRequestRoute(
		protocol.ClientMessage{
			Kind:      protocol.ClientMessageCallProcedure,
			RequestID: c.NextRequestID(),
			Procedure: procedure,
			Args:      args,
		},
		protocol.MessageKindProcedureResult,
		callResultCallback(callback),
	)
}

func (c *Connection) callWithRequestRoute(
	message protocol.ClientMessage,
	expectedKind protocol.MessageKind,
	callback callResultCallback,
) (uint32, error) {
	requestID := message.RequestID
	if callback != nil {
		c.callCallbacks.Store(requestID, callback)
		c.OnRequest(requestID, func(result protocol.RoutedMessage) {
			c.callCallbacks.Delete(requestID)
			c.ClearRequestRoute(requestID)
			if result.Kind != expectedKind {
				callback(result, fmt.Errorf("unexpected result kind: got %q want %q", result.Kind, expectedKind))
				return
			}
			callback(result, nil)
		})
	}

	if err := c.sendClientMessage(message); err != nil {
		if callback != nil {
			c.callCallbacks.Delete(requestID)
			c.ClearRequestRoute(requestID)
		}
		return requestID, err
	}

	return requestID, nil
}

func (c *Connection) sendClientMessage(message protocol.ClientMessage) error {
	encoded, err := c.messageEncoder(message)
	if err != nil {
		return fmt.Errorf("encode %s message: %w", message.Kind, err)
	}
	if err := c.SendBinary(encoded); err != nil {
		return fmt.Errorf("send %s message: %w", message.Kind, err)
	}
	return nil
}
