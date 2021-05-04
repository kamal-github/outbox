package datastore

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

func TestPostgres_Mine(t *testing.T) {
	type fields struct {
		db     *sql.DB
		table  string
		logger *zap.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []event.OutboxRow
		wantErr bool
	}{
		{
			name: "It fetched all (status==NULL) outbox rows",
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					m := []byte(`{"sqsCfg":{"DelaySeconds":null,"MessageAttributes":null,"MessageBody":"Hello","MessageDeduplicationId":null,"MessageGroupId":null,"MessageSystemAttributes":null,"QueueUrl":"https://sqs"}}`)

					rows := sqlmock.NewRows([]string{"id", "metadata", "payload"})
					rows = rows.AddRow(1, m, nil)

					mock.ExpectQuery("select").WillReturnRows(rows)

					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			want: []event.OutboxRow{
				{
					OutboxID: 1,
					Metadata: event.Metadata{
						SQSCfg: &event.SQSCfg{&sqs.SendMessageInput{
							MessageBody: aws.String("Hello"),
							QueueUrl:    aws.String("https://sqs"),
						}},
					},
				},
			},
			args: args{context.TODO()},
		},
		{
			name: "It returns multiple outbox rows  when invalid json metadata is given",
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					m1 := []byte(`{"sqsCfg":{"DelaySeconds":null,"MessageAttributes":null,"MessageBody":"Hello","MessageDeduplicationId":null,"MessageGroupId":null,"MessageSystemAttributes":null,"QueueUrl":"https://sqs"}}`)
					m2 := []byte(`{"sqsCfg":{"DelaySeconds":null,"MessageAttributes":null,"MessageBody":"Ciao","MessageDeduplicationId":null,"MessageGroupId":null,"MessageSystemAttributes":null,"QueueUrl":"https://anothersqs"}}`)

					rows := sqlmock.NewRows([]string{"id", "metadata", "payload"})
					rows = rows.AddRow(1, m1, nil)
					rows = rows.AddRow(2, m2, nil)

					mock.ExpectQuery("select").WillReturnRows(rows)

					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			want: []event.OutboxRow{
				{
					OutboxID: 1,
					Metadata: event.Metadata{
						SQSCfg: &event.SQSCfg{&sqs.SendMessageInput{
							MessageBody: aws.String("Hello"),
							QueueUrl:    aws.String("https://sqs"),
						}},
					},
				},
				{
					OutboxID: 2,
					Metadata: event.Metadata{
						SQSCfg: &event.SQSCfg{&sqs.SendMessageInput{
							MessageBody: aws.String("Ciao"),
							QueueUrl:    aws.String("https://anothersqs"),
						}},
					},
				},
			},
			args: args{context.TODO()},
		},
		{
			name: "It returns error when invalid json metadata is given",
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					m := []byte(`{}}`)

					rows := sqlmock.NewRows([]string{"id", "metadata", "payload"})
					rows = rows.AddRow(1, m, nil)

					mock.ExpectQuery("select").WillReturnRows(rows)

					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			wantErr: true,
			args:    args{context.TODO()},
		},
		{
			name: "It returns `ErrNoEvents` when no record found with (Status==NULL)",
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					mock.ExpectQuery("select").WillReturnError(sql.ErrNoRows)

					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			wantErr: true,
			args:    args{context.TODO()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Postgres{
				db:     tt.fields.db,
				table:  tt.fields.table,
				logger: tt.fields.logger,
			}
			defer p.db.Close()

			got, err := p.Mine(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Mine() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPostgres_Sweep(t *testing.T) {
	type fields struct {
		db     *sql.DB
		table  string
		logger *zap.Logger
	}
	type args struct {
		ctx        context.Context
		relayedIDs []int
		failedIDs  []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "It deletes the successfully dispatched rows",
			args: args{
				ctx:        context.TODO(),
				relayedIDs: []int{1, 2},
				failedIDs:  nil,
			},
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					mock.ExpectExec("DELETE FROM outbox").
						WillReturnResult(sqlmock.NewResult(0, 2))

					mock.ExpectationsWereMet()
					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
		},
		{
			name: "It returns error when only fewer rows are deleted from DB than dispatched",
			args: args{
				ctx:        context.TODO(),
				relayedIDs: []int{1, 2},
				failedIDs:  nil,
			},
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					mock.ExpectExec("DELETE FROM outbox").
						WillReturnResult(sqlmock.NewResult(0, 1))

					mock.ExpectationsWereMet()
					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			wantErr: true,
		},
		{
			name: "It updates the status to `1` (FAILED) for all the failed dispatched rows",
			args: args{
				ctx:        context.TODO(),
				failedIDs:  []int{1, 2},
				relayedIDs: nil,
			},
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					mock.ExpectExec("UPDATE outbox").
						WillReturnResult(sqlmock.NewResult(0, 2))

					mock.ExpectationsWereMet()
					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
		},
		{
			name: "It returns error when update the status to `1` (FAILED) for fewer than dispatched rows",
			args: args{
				ctx:        context.TODO(),
				relayedIDs: []int{1, 2},
				failedIDs:  nil,
			},
			fields: fields{
				db: func() *sql.DB {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
					}

					mock.ExpectExec("UPDATE outbox").
						WillReturnResult(sqlmock.NewResult(0, 1))

					mock.ExpectationsWereMet()
					return db
				}(),
				table:  outboxTable,
				logger: logger,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Postgres{
				db:     tt.fields.db,
				table:  tt.fields.table,
				logger: tt.fields.logger,
			}
			defer p.db.Close()

			if err := p.Sweep(tt.args.ctx, tt.args.relayedIDs, tt.args.failedIDs); (err != nil) != tt.wantErr {
				t.Errorf("Sweep() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
