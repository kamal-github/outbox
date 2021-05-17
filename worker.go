package outbox_worker

import (
	"context"
	"errors"
	"time"

	"github.com/kamal-github/outbox/backend"
	"github.com/kamal-github/outbox/datastore"
	"go.uber.org/zap"
)

type Worker struct {
	MineSweeper  datastore.MineSweeper
	Dispatcher   backend.Dispatcher
	MineInterval time.Duration

	Logger *zap.Logger
}

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
		// Validation (e.g metadata incorrect format) error or network error.
		if err != nil {
			w.Logger.Error("failed while collecting event from datastore", zap.Error(err))
			// increase counter for metrics
			continue
		}

		if err = w.Dispatcher.Dispatch(ctx, events); err != nil {
			w.Logger.Error("failed while sending event to Dispatcher", zap.Error(err), zap.String("backend", "rabbitmq"))
			// increase counter for metrics
			continue
		}
	}
}
