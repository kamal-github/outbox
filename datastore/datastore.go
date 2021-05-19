// Package datastore maintains all the datastorage implementation.
package datastore

import (
	"context"
	"errors"
	"io"

	"github.com/kamal-github/outbox/event"
)

// ErrNoEvents is an error returned when no ready outbox rows available
// after mining from DB.
var ErrNoEvents = errors.New("no events")

// Status represents the outbox row status.
type Status int

const (
	// InProcess represents outbox row is already being processed by worker.
	InProcess Status = iota + 1

	// Failed represents failed to publish outbox row.
	Failed
)

// MineSweeper should be implemented by Datastore.
type MineSweeper interface {
	// Mine mines the ready to dispatch outbox rows from DB.
	// Implementor should marks the fetched rows Status to InProcess so as to avoid being same records re-fetched in another
	// iteration by Worker.
	Mine(ctx context.Context) ([]event.OutboxRow, error)

	// Sweep delete or mark as Failed on publish occur or fails respectively.
	Sweep(context.Context, []int, []int) error
	io.Closer
}
