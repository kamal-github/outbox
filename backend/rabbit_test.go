package backend

import (
	"context"
	"os"
	"testing"

	"github.com/streadway/amqp"

	"github.com/golang/mock/gomock"

	sweepermock "github.com/kamal-github/outbox/backend/mocks"

	"github.com/angora-go/angora"

	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

func TestRabbitMQ_Dispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := sweepermock.NewMockSweeper(ctrl)
	m.EXPECT().Sweep(context.TODO(), nil, nil).Return(nil).Times(1)

	r, err := NewRabbitMQ(os.Getenv("BACKEND_URL"), m, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	type fields struct {
		conn    *angora.Connection
		logger  *zap.Logger
		sweeper Sweeper
	}
	type args struct {
		ctx  context.Context
		rows []event.OutboxRow
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "It dispatches the message to the exchange",
			args: args{
				ctx: context.TODO(),
				rows: []event.OutboxRow{
					{
						OutboxID: 1,
						Metadata: event.Metadata{
							RabbitCfg: &event.RabbitCfg{
								Exchange:   "test.exchange",
								RoutingKey: "test.routingKey",
								Publishing: amqp.Publishing{
									Body: []byte("test payload"),
								},
							},
						},
					},
					{
						OutboxID: 2,
						Metadata: event.Metadata{
							RabbitCfg: &event.RabbitCfg{
								Exchange:   "test.exchange",
								RoutingKey: "test.routingKey",
								Publishing: amqp.Publishing{
									Body: []byte("test payload"),
								},
							},
						},
					},
					{
						OutboxID: 3,
						Metadata: event.Metadata{
							RabbitCfg: &event.RabbitCfg{
								Exchange:   "test.exchange",
								RoutingKey: "test.routingKey",
								Publishing: amqp.Publishing{
									Body: []byte("test payload"),
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.Dispatch(tt.args.ctx, tt.args.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dispatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
