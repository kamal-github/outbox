package datastore

import (
	"context"
	"errors"
	"io"

	"github.com/kamal-github/outbox/event"
)

var ErrNoEvents = errors.New("no events")

type Status int

const (
	Failed Status = iota + 1
	InProcess
)

type MineSweeper interface {
	Mine(ctx context.Context) ([]event.OutboxRow, error)
	Sweep(context.Context, []int, []int) error
	io.Closer
}
