package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kamal-github/outbox/event"
	"github.com/kamal-github/outbox/internal/backoff"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMQ struct {
	backendURL string

	connectionM sync.RWMutex
	conn        *amqp.Connection
	ch          *amqp.Channel

	amqpErrCh    chan *amqp.Error
	pubConfirmCh chan *amqp.Confirmation

	logger *zap.Logger
}

// Dispatch relays the message to RabbitMQ exchange.
func (r *RabbitMQ) Dispatch(ctx context.Context, rows []event.OutboxRow) ([]int, []int, error) {
	successIDs := make([]int, 0)
	failedIDs := make([]int, 0)

	for _, row := range rows {
		cfg := row.Metadata.RabbitCfg
		amqpPub := amqpPublishing(cfg.Publishing, row)
		r.connectionM.RLock()
		if err := r.ch.Publish(cfg.Exchange, cfg.RoutingKey, cfg.Mandatory, cfg.Immediate, amqpPub); err != nil {
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

// NewRabbitMQ
// TODO: take TLS config as optional parameter.
func NewRabbitMQ(backendURL string, logger *zap.Logger) (Dispatcher, error) {
	r := &RabbitMQ{
		backendURL: backendURL,
		amqpErrCh:  make(chan *amqp.Error, 1),
		logger:     logger,
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

func amqpPublishing(pub event.Publishing, row event.OutboxRow) amqp.Publishing {
	ts := time.Time{}
	if pub.Timestamp != nil {
		ts = *pub.Timestamp
	}
	amqpPub := amqp.Publishing{
		Headers:         pub.Headers,
		ContentType:     pub.ContentType,
		ContentEncoding: pub.ContentEncoding,
		DeliveryMode:    pub.DeliveryMode,
		Priority:        pub.Priority,
		CorrelationId:   pub.CorrelationId,
		ReplyTo:         pub.ReplyTo,
		Expiration:      pub.Expiration,
		MessageId:       pub.MessageId,
		Timestamp:       ts,
		Type:            pub.Type,
		UserId:          pub.UserId,
		AppId:           pub.AppId,
		Body:            row.Payload,
	}
	return amqpPub
}
