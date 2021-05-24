package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/kamal-github/outbox/datastore"
	"github.com/kamal-github/outbox/pubsub"
	"go.uber.org/zap"
)

// Worker is the outbox worker which runs repeatedly until asked to stop.
type Worker struct {
	MineSweeper  datastore.MineSweeper
	Dispatcher   pubsub.Dispatcher
	MineInterval time.Duration

	Logger *zap.Logger
}

// Start starts the outbox worker and iterative looks for new outbox rows (ready to process)
// after each given MineInterval and publishes to one of the configured Messaging system.
//
// When no ready to process message are found, it keep looking for new ones.
//
// Exit as soon as ctx is cancelled.
func (w Worker) Start(ctx context.Context, done chan<- struct{}) {
	ticker := time.NewTicker(w.MineInterval)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			done <- struct{}{}
			return
		default:
		}

		ctx := context.Background()
		events, err := w.MineSweeper.Mine(ctx)
		if errors.Is(err, datastore.ErrNoEvents) {
			w.Logger.Error("failed while Err no event collecting event from datastore", zap.Error(err))
			continue
		}
		// Validation error (e.g metadata incorrect format) or network error.
		if err != nil {
			w.Logger.Error("failed while collecting event from datastore", zap.Error(err))
			continue
		}

		if err = w.Dispatcher.Dispatch(ctx, events); err != nil {
			w.Logger.Error("failed while sending event to Dispatcher", zap.Error(err), zap.String("pubsub", "rabbitmq"))
		}
	}
}
