package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/kamal-github/outbox/event"
	"github.com/kamal-github/outbox/internal/backoff"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMQ struct {
	backendURL string

	tls         *tls.Config
	connectionM sync.RWMutex
	conn        *amqp.Connection
	ch          *amqp.Channel

	amqpErrCh    chan *amqp.Error
	pubConfirmCh chan *amqp.Confirmation

	logger *zap.Logger
}

// Dispatch relays the message to RabbitMQ exchange.
func (r *RabbitMQ) Dispatch(ctx context.Context, rows []event.OutboxRow) (SuccessIDs, FailedIDs, error) {
	successIDs := make([]int, 0)
	failedIDs := make([]int, 0)

	for _, row := range rows {
		cfg := row.Metadata.RabbitCfg
		r.connectionM.RLock()
		if err := r.ch.Publish(cfg.Exchange, cfg.RoutingKey, cfg.Mandatory, cfg.Immediate, cfg.Publishing); err != nil {
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}
		r.connectionM.RUnlock()

		successIDs = append(successIDs, row.OutboxID)
	}

	return successIDs, failedIDs, nil
}

func (r *RabbitMQ) Close() error {
	return r.conn.Close()
}

type Option func(mq *RabbitMQ) error

func WithTLS(t *tls.Config) Option {
	return func(mq *RabbitMQ) error {
		if t == nil {
			return fmt.Errorf("invalid tls config, %v", t)
		}

		mq.tls = t

		return nil
	}
}

// NewRabbitMQ creates a RabbitMQ dispatcher
func NewRabbitMQ(backendURL string, logger *zap.Logger, opts ...Option) (Dispatcher, error) {
	r := &RabbitMQ{
		backendURL: backendURL,
		amqpErrCh:  make(chan *amqp.Error, 1),
		logger:     logger,
	}

	angora.

	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	go r.reconnect()

	return r, nil
}

func (r *RabbitMQ) reconnect() {
	var retried int

	for {
		if !r.connect() {
			retried++
			backoff.ExponentialWait(retried, 20)
			continue
		}

		select {
		case <-r.amqpErrCh:
		}
	}
}

func (r *RabbitMQ) connect() bool {
	if err := r.open(); err != nil {
		return false
	}

	return true
}

// open dials to Rabbit Server and creates a connection and set it
// to client.Connection.
//
// This also sets up a ConnectionClose notifier.
func (r *RabbitMQ) open() error {
	conn, err := amqp.DialTLS(r.backendURL, nil)
	if err != nil {
		return fmt.Errorf("client: cannot dial amqp connection: %w", err)
	}

	// Note - Create a new chan, as the old chan got closed on RabbitMQ server shutdown or
	// during the connection/channel close. so a new *amqp.Error chan is required.
	r.amqpErrCh = make(chan *amqp.Error, 1)

	r.connectionM.Lock()
	defer r.connectionM.Unlock()

	r.conn = conn
	r.ch, err = conn.Channel()
	if err != nil {
		return err
	}
	r.ch.NotifyClose(r.amqpErrCh)

	return nil
}
