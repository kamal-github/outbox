package pubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/golang/mock/gomock"

	sweepermock "github.com/kamal-github/outbox/pubsub/mocks"

	"github.com/kamal-github/outbox/pubsub"

	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

func TestRabbitMQ_Dispatch(t *testing.T) {
	type fields struct {
		sweeper func() (pubsub.Sweeper, *gomock.Controller)
		logger  *zap.Logger
	}
	type args struct {
		ctx  context.Context
		rows func() []event.OutboxRow
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		queueManager func() (*string, func() error)
	}{
		{
			name: "it publishes to SQS queue and sweeps with dispatched IDs",
			queueManager: func() (*string, func() error) {
				_, cleaner := setup(t, "test-unit-queue", "test.unit.exchange", "test.unit.routingKey")

				return nil, cleaner
			},
			args: args{
				ctx: context.TODO(),
				rows: func() []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								RabbitCfg: &event.RabbitCfg{
									Exchange:   "test.unit.exchange",
									RoutingKey: "test.unit.routingKey",
									Publishing: amqp.Publishing{
										ContentType:  "application/json",
										DeliveryMode: amqp.Transient,
										Timestamp:    time.Now().UTC(),
										Body:         []byte("hello from rabbit unit test"),
									},
								},
							},
						},
					}
				},
			},
			fields: fields{
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)
					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), nil, nil).Return(nil)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},
		{
			name: "it publishes to SQS queue and sweeps with dispatched IDs",
			queueManager: func() (*string, func() error) {
				_, cleaner := setup(t, "test-unit-queue", "test.unit.exchange", "test.unit.routingKey")

				return nil, cleaner
			},
			args: args{
				ctx: context.TODO(),
				rows: func() []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								RabbitCfg: &event.RabbitCfg{
									Exchange:   "test.unit.exchange",
									RoutingKey: "test.unit.routingKey",
									Publishing: amqp.Publishing{
										ContentType:  "application/json",
										DeliveryMode: amqp.Transient,
										Timestamp:    time.Now().UTC(),
										Body:         []byte("hello from rabbit unit test"),
									},
								},
							},
						},
					}
				},
			},
			fields: fields{
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)
					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), nil, nil).Return(nil)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},

		{
			name: "it sweeps with failedIDs, when metadata is mis-configured",
			queueManager: func() (*string, func() error) {
				return nil, func() error {
					return nil
				}
			},
			args: args{
				ctx: context.TODO(),
				rows: func() []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								RabbitCfg: nil,
							},
						},
					}
				},
			},
			fields: fields{
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)
					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), nil, []int{100}).Return(nil)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},
		{
			name: "it returns NO error, when empty outbox rows",
			queueManager: func() (*string, func() error) {
				return nil, func() error {
					return nil
				}
			},
			args: args{
				ctx: context.TODO(),
				rows: func() []event.OutboxRow {
					return []event.OutboxRow{}
				},
			},
			fields: fields{
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)
					d := sweepermock.NewMockSweeper(ctrl)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},
		{
			name: "it returns error, when Sweep fails",
			queueManager: func() (*string, func() error) {
				return nil, func() error {
					return nil
				}
			},
			args: args{
				ctx: context.TODO(),
				rows: func() []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								RabbitCfg: nil,
							},
						},
					}
				},
			},
			fields: fields{
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)
					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), nil, []int{100}).Return(nil)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, ctrl := tt.fields.sweeper()
			defer ctrl.Finish()

			r, err := pubsub.NewRabbitMQ(
				envCfg.RabbitMQURI,
				sw,
				tt.fields.logger,
			)
			if err != nil {
				t.Error(err)
			}
			defer r.Close()

			if err := r.Dispatch(tt.args.ctx, tt.args.rows()); (err != nil) != tt.wantErr {
				t.Errorf("Dispatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func newConnection(t testing.TB) *amqp.Connection {
	t.Helper()

	conn, err := amqp.Dial(envCfg.RabbitMQURI)
	assert.NoError(t, err)

	return conn
}

func setup(t testing.TB, qn, ex, rKey string) (*amqp.Channel, func() error) {
	t.Helper()

	conn := newConnection(t)
	ch, _ := conn.Channel()
	declareExchange(t, ch, ex)
	q, closeFn := declareQueue(t, ch, qn)

	if !(qn == "" || ex == "" || rKey == "") {
		assert.NoError(t, ch.QueueBind(q.Name, rKey, ex, false, nil))
	}

	return ch, func() error {
		closeFn()
		ch.Close()
		return conn.Close()
	}
}

func declareExchange(t testing.TB, ch *amqp.Channel, ex string) {
	if ex == "" {
		t.Log("no exchange given")
		return
	}

	assert.NoError(t, ch.ExchangeDeclare(ex, amqp.ExchangeTopic, true, false, false, false, nil))
}

func declareQueue(t testing.TB, ch *amqp.Channel, qn string) (amqp.Queue, func()) {
	if qn == "" {
		t.Log("no queue name given")
		return amqp.Queue{}, func() {}
	}

	q, err := ch.QueueDeclare(qn, true, false, false, false, nil)
	assert.NoError(t, err)

	return q, func() {
		_, err := ch.QueueDelete(q.Name, false, false, false)
		assert.NoError(t, err)
	}
}
