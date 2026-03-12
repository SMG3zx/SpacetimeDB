package client

import "github.com/clockworklabs/spacetimedb-go/protocol"

func (c *DbConnection) handleMessage(msg protocol.ServerMessage) error {
	switch msg.Kind {
	case protocol.ServerMessageTransactionUpdate:
		return c.applyTransactionUpdate(msg.TransactionUpdate.TransactionUpdate)
	case protocol.ServerMessageSubscribeApplied:
		if err := c.applySubscribeApplied(msg.SubscribeApplied); err != nil {
			return err
		}
		c.subscriptionManager.handleApplied(msg.SubscribeApplied.QuerySetId.ID)
	case protocol.ServerMessageUnsubscribeApplied:
		c.subscriptionManager.handleUnsubscribeApplied(msg.UnsubscribeApplied.QuerySetId.ID)
	case protocol.ServerMessageSubscriptionError:
		qsid := msg.SubscriptionError.QuerySetId.ID
		c.subscriptionManager.handleError(qsid, msg.SubscriptionError.Error)
	case protocol.ServerMessageReducerResult:
		// Reducer results are fire-and-forget; callers use CallReducer and handle via callbacks.
	case protocol.ServerMessageOneOffQueryResult:
		m := msg.OneOffQueryResult
		c.mu.Lock()
		ch, ok := c.pendingOneOff[m.RequestId]
		if ok {
			delete(c.pendingOneOff, m.RequestId)
		}
		c.mu.Unlock()
		if ok {
			ch <- m
		}
	case protocol.ServerMessageProcedureResult:
		m := msg.ProcedureResult
		c.mu.Lock()
		ch, ok := c.pendingProcedures[m.RequestId]
		if ok {
			delete(c.pendingProcedures, m.RequestId)
		}
		c.mu.Unlock()
		if ok {
			ch <- m
		}
	}
	return nil
}

func (c *DbConnection) applyTransactionUpdate(tx protocol.TransactionUpdate) error {
	for _, qsu := range tx.QuerySets {
		for _, tbl := range qsu.Tables {
			h, ok := c.tableHandlers[tbl.TableName]
			if !ok {
				continue
			}
			for _, rows := range tbl.Rows {
				switch rows.Kind {
				case protocol.TableUpdateRowsPersistent:
					if err := h.ApplyInserts(&rows.PersistentTable.Inserts); err != nil {
						return err
					}
					if err := h.ApplyDeletes(&rows.PersistentTable.Deletes); err != nil {
						return err
					}
				case protocol.TableUpdateRowsEvent:
					if err := h.ApplyInserts(&rows.EventTable.Events); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (c *DbConnection) applySubscribeApplied(msg *protocol.SubscribeAppliedMsg) error {
	for _, tableRows := range msg.Rows.Tables {
		h, ok := c.tableHandlers[tableRows.Table]
		if !ok {
			continue
		}
		if err := h.ApplyInserts(&tableRows.Rows); err != nil {
			return err
		}
	}
	return nil
}
