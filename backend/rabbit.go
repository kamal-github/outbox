package backend

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/angora-go/angora"
	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

type RabbitMQ struct {
	backendURL string
	tls        *tls.Config
	conn       *angora.Connection
	sweeper    Sweeper

	logger *zap.Logger
}

// Dispatch relays the message to RabbitMQ exchange.
// Successful publish is considered when publish confirm is received, and then sweeper sweeps the successful outbox rows.
// Failed to publish are immediately marked as "Failed" by sweeper.
func (r *RabbitMQ) Dispatch(ctx context.Context, rows []event.OutboxRow) (err error) {
	failedIDs := make([]int, 0)

	for _, row := range rows {
		cfg := row.Metadata.RabbitCfg
		if err = r.conn.Publish(
			ctx,
			cfg.Exchange,
			angora.ProducerConfig{
				RoutingKey: cfg.RoutingKey,
				Mandatory:  cfg.Mandatory,
				Immediate:  cfg.Immediate,
			},
			cfg.Publishing,
		); err != nil {
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}
	}

	return r.sweeper.Sweep(ctx, nil, failedIDs)
}

// Close gracefully closes the underlying amqp.Connection through angora.
func (r *RabbitMQ) Close() error {
	return r.conn.Shutdown(context.Background())
}

type Option func(mq *RabbitMQ) error

func WithTLS(t *tls.Config) Option {
	return func(r *RabbitMQ) error {
		if t == nil {
			return fmt.Errorf("invalid tls config, %v", t)
		}

		r.tls = t

		return nil
	}
}

// NewRabbitMQ creates a RabbitMQ dispatcher.
func NewRabbitMQ(amqpURL string, sw Sweeper, logger *zap.Logger, opts ...Option) (*RabbitMQ, error) {
	var err error

	r := &RabbitMQ{
		backendURL: amqpURL,
		logger:     logger,
		sweeper:    sw,
	}

	for _, o := range opts {
		if err = o(r); err != nil {
			return nil, err
		}
	}

	onPubConfirmAckFn := func(deliveryTaggedData interface{}, ack bool) {
		var successID, failedID int
		if ack {
			successID = deliveryTaggedData.(int)
		} else {
			failedID = deliveryTaggedData.(int)
		}

		if err := r.sweeper.Sweep(context.Background(), []int{successID}, []int{failedID}); err != nil {
			logger.Error("failed to sweep", zap.Int("successID", successID), zap.Int("failedID", failedID), zap.Bool("Ack", ack))
		}
	}

	angOpts := []angora.Option{
		angora.WithPublishConfirm(onPubConfirmAckFn),
	}

	if r.tls != nil {
		angOpts = append(angOpts, angora.WithTLSConfig(r.tls))
	}

	r.conn, err = angora.NewConnection(
		amqpURL,
		angOpts...,
	)
	if err != nil {
		return nil, err
	}

	return r, nil
}
