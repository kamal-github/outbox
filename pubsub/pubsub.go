package pubsub

import (
	"context"
	"io"

	"github.com/kamal-github/outbox/event"
)

// Dispatcher should be implemented by client which dispatches the outbox rows to its Queuing Server.
type Dispatcher interface {
	// Dispatch sends the outbox rows to the underlying queuing system.
	Dispatch(ctx context.Context, rows []event.OutboxRow) error

	io.Closer
}

// Sweeper represents the datastore containing outbox rows/events.
type Sweeper interface {
	// Sweep delete the outbox rows from the configured datastore.
	Sweep(ctx context.Context, dispatchedIDs, failedIDs []int) error
}
