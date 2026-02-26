package protocol

import "fmt"

type MessageKind string

const (
	MessageKindInitialConnection  MessageKind = "initial_connection"
	MessageKindSubscribeApplied   MessageKind = "subscribe_applied"
	MessageKindUnsubscribeApplied MessageKind = "unsubscribe_applied"
	MessageKindSubscriptionError  MessageKind = "subscription_error"
	MessageKindTransactionUpdate  MessageKind = "transaction_update"
	MessageKindOneOffQueryResult  MessageKind = "one_off_query_result"
	MessageKindReducerResult      MessageKind = "reducer_result"
	MessageKindProcedureResult    MessageKind = "procedure_result"
	MessageKindUnknown            MessageKind = "unknown"
)

type RoutedMessage struct {
	Kind      MessageKind
	RequestID *uint32
	QueryID   *uint32
	Payload   any
}

func (m RoutedMessage) Validate() error {
	if m.Kind == "" {
		return fmt.Errorf("routed message kind is required")
	}
	return nil
}

type MessageDecoder func(payload []byte) (RoutedMessage, error)

type RouteHandler func(message RoutedMessage)
