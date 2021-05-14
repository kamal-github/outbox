package backend

import (
	"context"
	"os"
	"testing"

	"github.com/angora-go/angora"

	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

func TestRabbitMQ_Dispatch(t *testing.T) {
	r, err := NewRabbitMQ(os.Getenv("RABBIT_URL"), nil, zap.NewNop())
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
		fields  fields
		wantErr bool
	}{
		{
			name: "It dispatches the message to the exchange",
			args: args{
				ctx:  context.TODO(),
				rows: []event.OutboxRow{},
			},
			fields: fields{
				conn:    nil,
				logger:  zap.NewNop(),
				sweeper: nil,
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
