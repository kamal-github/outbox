package backend

import (
	"context"
	"io"

	"github.com/kamal-github/outbox/event"
)

const (
	RABBITMQ = "rabbitmq"
	SQS      = "sqs"
)

type (
	SuccessIDs []int
	FailedIDs  []int
)

type Dispatcher interface {
	Dispatch(ctx context.Context, rows []event.OutboxRow) (SuccessIDs, FailedIDs, error)
	io.Closer
}

type Sweeper interface {
	Sweep(ctx context.Context, relayedIDs []int, failedIDs []int) error
}
