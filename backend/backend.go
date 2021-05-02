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

type Dispatcher interface {
	Dispatch(ctx context.Context, rows []event.OutboxRow) ([]int, []int, error)
	io.Closer
}
