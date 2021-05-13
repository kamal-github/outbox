package backend

import (
	"context"
	"time"

	"github.com/kamal-github/outbox/event"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMQ struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	logger *zap.Logger
}

// Dispatch relays the message to RabbitMQ exchange.
func (r *RabbitMQ) Dispatch(ctx context.Context, rows []event.OutboxRow) (SuccessIDs, FailedIDs, error) {
	successIDs := make([]int, 0)
	failedIDs := make([]int, 0)

	for _, row := range rows {
		cfg := row.Metadata.RabbitCfg
		amqpPub := amqpPublishing(cfg.Publishing, row)

		if err := r.ch.Publish(
			cfg.Exchange,
			cfg.RoutingKey,
			cfg.Mandatory,
			cfg.Immediate,
			amqpPub,
		); err != nil {
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		successIDs = append(successIDs, row.OutboxID)
	}

	return successIDs, failedIDs, nil
}

func (r *RabbitMQ) Close() error {
	return r.conn.Close()
}

func NewRabbitMQ(c *amqp.Connection, l *zap.Logger) (Dispatcher, error) {
	r := &RabbitMQ{
		conn:   c,
		logger: l,
	}

	var err error
	r.ch, err = c.Channel()
	if err != nil {
		return nil, err
	}

	return r, nil
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
