package backend

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/kamal-github/outbox/event"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

func TestRabbitMQ_Dispatch(t *testing.T) {
	conn, err := amqp.Dial(os.Getenv("RABBIT_URL"))
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRabbitMQ(conn, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	type fields struct {
		conn   *amqp.Connection
		ch     *amqp.Channel
		logger *zap.Logger
	}
	type args struct {
		ctx  context.Context
		rows []event.OutboxRow
	}
	tests := []struct {
		name    string
		args    args
		want    SuccessIDs
		want1   FailedIDs
		wantErr bool
	}{
		{
			name: "It dispatches the message to the exchange",
			args: args{
				ctx:  context.TODO(),
				rows: []event.OutboxRow{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sIDs, fIDs, err := r.Dispatch(tt.args.ctx, tt.args.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dispatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(sIDs, tt.want) {
				t.Errorf("Dispatch() got = %v, want %v", sIDs, tt.want)
			}
			if !reflect.DeepEqual(fIDs, tt.want1) {
				t.Errorf("Dispatch() got1 = %v, want %v", fIDs, tt.want1)
			}
		})
	}
}
