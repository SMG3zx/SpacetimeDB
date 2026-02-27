package connection

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb/sdks/go/events"
	"github.com/clockworklabs/spacetimedb/sdks/go/internal/protocol"
)

type ReducerResultCallback = events.ReducerResultCallback
type ProcedureResultCallback = events.ProcedureResultCallback

type callResultCallback = events.ResultCallback

func (c *Connection) CallReducer(reducer string, args []byte, callback ReducerResultCallback) (uint32, error) {
	if reducer == "" {
		return 0, newInvalidArgument("call_reducer", "reducer name is required")
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
		return 0, newInvalidArgument("call_procedure", "procedure name is required")
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
				callback(result, newUnexpectedKind("call_result", string(result.Kind), string(expectedKind)))
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
		return wrapError(ErrorEncodeFailed, fmt.Sprintf("encode_%s", message.Kind), err)
	}
	if err := c.SendBinary(encoded); err != nil {
		return wrapError(ErrorSendFailed, fmt.Sprintf("send_%s", message.Kind), err)
	}
	return nil
}
