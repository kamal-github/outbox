package backend

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/angora-go/angora"
	"github.com/kamal-github/outbox/event"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMQ struct {
	backendURL string
	tls        *tls.Config
	conn       *angora.Connection
	sweeper    Sweeper

	logger *zap.Logger
}

const outboxID = "outboxID"

// Dispatch relays the message to RabbitMQ exchange.
// Successful publish is considered when publish confirm is received, and then sweeper sweeps the successful outbox rows.
// Failed to publish are immediately marked as "Failed" by sweeper.
func (r *RabbitMQ) Dispatch(ctx context.Context, rows []event.OutboxRow) (err error) {
	var failedIDs []int

	for _, row := range rows {
		cfg := row.Metadata.RabbitCfg
		if cfg == nil {
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		// Adding outbox id to the header so as to identify which outbox row is
		// publisher confirmation ACKed.
		h := amqp.Table{outboxID: row.OutboxID}
		cfg.Publishing.Headers = h

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

	onPubConfirmAckFn := func(pub interface{}, ack bool) {
		var (
			up angora.UnconfirmedPub
			ok bool
		)

		if up, ok = pub.(angora.UnconfirmedPub); !ok {
			logger.Error("received invalid type, expected UnconfirmedPub")
		}

		var oid int
		id := up.Publishing.Headers[outboxID]
		if oid, ok = id.(int); !ok {
			logger.Error("received invalid type, expected int for outbox id")
		}

		var successIDs, failedIDs []int
		if ack {
			successIDs = append(successIDs, oid)
		} else {
			failedIDs = append(failedIDs, oid)
		}

		if err := r.sweeper.Sweep(context.Background(), successIDs, failedIDs); err != nil {
			logger.Error("failed to sweep", zap.Ints("successIDs", successIDs), zap.Ints("failedID", failedIDs), zap.Bool("Ack", ack))
		}
	}

	angOpts := []angora.Option{
		angora.WithChannelPool(),
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
