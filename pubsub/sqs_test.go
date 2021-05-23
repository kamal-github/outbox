package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kamal-github/outbox/internal/config"

	"github.com/golang/mock/gomock"

	sweepermock "github.com/kamal-github/outbox/pubsub/mocks"

	"github.com/kamal-github/outbox/pubsub"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

var envCfg config.ENV

func init() {
	var err error
	envCfg, err = config.Process()
	if err != nil {
		panic(err)
	}
}

func TestSimpleQueueService_Dispatch(t *testing.T) {
	type fields struct {
		sqsConn *sqs.SQS
		sweeper func() (pubsub.Sweeper, *gomock.Controller)
		logger  *zap.Logger
	}
	type args struct {
		ctx  context.Context
		rows func(queueURL *string) []event.OutboxRow
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		queueManager func(*sqs.SQS) (*string, func())
	}{
		{
			name: "it publishes to SQS queue and sweeps with dispatched IDs",
			queueManager: func(sqsService *sqs.SQS) (*string, func()) {
				o, cleaner := createSQSQueue(t, sqsService)
				return o.QueueUrl, cleaner
			},
			args: args{
				ctx: context.TODO(),
				rows: func(queueURL *string) []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								SQSCfg: &event.SQSCfg{
									SendMessageInput: &sqs.SendMessageInput{
										MessageBody: aws.String("hello from sqs unit test"),
										QueueUrl:    queueURL,
									},
								},
							},
						},
						{
							OutboxID: 101,
							Metadata: event.Metadata{
								SQSCfg: &event.SQSCfg{
									SendMessageInput: &sqs.SendMessageInput{
										MessageBody: aws.String("hello from sqs unit test"),
										QueueUrl:    queueURL,
									},
								},
							},
						},
					}
				},
			},
			fields: fields{
				sqsConn: func() *sqs.SQS {
					sqsHost := envCfg.SQSURI

					ses := session.Must(session.NewSessionWithOptions(session.Options{
						SharedConfigState: session.SharedConfigDisable,
						Config: aws.Config{
							Credentials: credentials.NewStaticCredentials("test", "test", ""),
							Endpoint:    aws.String(sqsHost),
							Region:      aws.String("eu-central-1"),
						},
					}))

					return sqs.New(ses, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
				}(),
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)

					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), []int{100, 101}, nil).Return(nil)

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
		},
		{
			name: "it returns NO error, when empty outbox rows",
			args: args{
				ctx: context.TODO(),
				rows: func(queueURL *string) []event.OutboxRow {
					return []event.OutboxRow{}
				},
			},
			queueManager: func(sqsService *sqs.SQS) (*string, func()) {
				o, cleaner := createSQSQueue(t, sqsService)
				return o.QueueUrl, cleaner
			},
			fields: fields{
				sqsConn: func() *sqs.SQS {
					sqsHost := envCfg.SQSURI

					ses := session.Must(session.NewSessionWithOptions(session.Options{
						SharedConfigState: session.SharedConfigDisable,
						Config: aws.Config{
							Credentials: credentials.NewStaticCredentials("test", "test", ""),
							Endpoint:    aws.String(sqsHost),
							Region:      aws.String("eu-central-1"),
						},
					}))

					return sqs.New(ses, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
				}(),
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
			name: "it sweeps with failedIDs, when sending to SQS queue fails",
			args: args{
				ctx: context.TODO(),
				rows: func(queueURL *string) []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								SQSCfg: &event.SQSCfg{
									SendMessageInput: &sqs.SendMessageInput{
										MessageBody: aws.String("hello from sqs unit test"),
										QueueUrl:    queueURL,
									},
								},
							},
						},
					}
				},
			},
			queueManager: func(sqsService *sqs.SQS) (*string, func()) {
				return aws.String("non-existing-queue"), func() {}
			},
			fields: fields{
				sqsConn: func() *sqs.SQS {
					sqsHost := envCfg.SQSURI

					ses := session.Must(session.NewSessionWithOptions(session.Options{
						SharedConfigState: session.SharedConfigDisable,
						Config: aws.Config{
							Credentials: credentials.NewStaticCredentials("test", "test", ""),
							Endpoint:    aws.String(sqsHost),
							Region:      aws.String("eu-central-1"),
						},
					}))

					return sqs.New(ses, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
				}(),
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
			name: "it sweeps with failedIDs, when metadata is mis-configured",
			args: args{
				ctx: context.TODO(),
				rows: func(queueURL *string) []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								SQSCfg: nil,
							},
						},
					}
				},
			},
			queueManager: func(sqsService *sqs.SQS) (*string, func()) {
				return aws.String("non-existing-queue"), func() {}
			},
			fields: fields{
				sqsConn: func() *sqs.SQS {
					sqsHost := envCfg.SQSURI

					ses := session.Must(session.NewSessionWithOptions(session.Options{
						SharedConfigState: session.SharedConfigDisable,
						Config: aws.Config{
							Credentials: credentials.NewStaticCredentials("test", "test", ""),
							Endpoint:    aws.String(sqsHost),
							Region:      aws.String("eu-central-1"),
						},
					}))

					return sqs.New(ses, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
				}(),
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
			name: "it returns error, when Sweep fails",
			queueManager: func(sqsService *sqs.SQS) (*string, func()) {
				o, cleaner := createSQSQueue(t, sqsService)
				return o.QueueUrl, cleaner
			},
			args: args{
				ctx: context.TODO(),
				rows: func(queueURL *string) []event.OutboxRow {
					return []event.OutboxRow{
						{
							OutboxID: 100,
							Metadata: event.Metadata{
								SQSCfg: &event.SQSCfg{
									SendMessageInput: &sqs.SendMessageInput{
										MessageBody: aws.String("hello from sqs unit test"),
										QueueUrl:    queueURL,
									},
								},
							},
						},
						{
							OutboxID: 101,
							Metadata: event.Metadata{
								SQSCfg: &event.SQSCfg{
									SendMessageInput: &sqs.SendMessageInput{
										MessageBody: aws.String("hello from sqs unit test"),
										QueueUrl:    queueURL,
									},
								},
							},
						},
					}
				},
			},
			fields: fields{
				sqsConn: func() *sqs.SQS {
					sqsHost := envCfg.SQSURI

					ses := session.Must(session.NewSessionWithOptions(session.Options{
						SharedConfigState: session.SharedConfigDisable,
						Config: aws.Config{
							Credentials: credentials.NewStaticCredentials("test", "test", ""),
							Endpoint:    aws.String(sqsHost),
							Region:      aws.String("eu-central-1"),
						},
					}))

					return sqs.New(ses, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
				}(),
				sweeper: func() (pubsub.Sweeper, *gomock.Controller) {
					ctrl := gomock.NewController(t)

					d := sweepermock.NewMockSweeper(ctrl)

					d.EXPECT().Sweep(context.TODO(), []int{100, 101}, nil).Return(errors.New("any error"))

					return d, ctrl
				},
				logger: zap.NewNop(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, ctrl := tt.fields.sweeper()
			defer ctrl.Finish()

			sqsConn := tt.fields.sqsConn
			q, cleaner := tt.queueManager(sqsConn)
			defer cleaner()

			s, err := pubsub.NewSimpleQueueService(
				sqsConn,
				sw,
				tt.fields.logger,
			)
			if err != nil {
				t.Error(err)
			}

			if err := s.Dispatch(tt.args.ctx, tt.args.rows(q)); (err != nil) != tt.wantErr {
				t.Errorf("Dispatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func uniqueString(str string) string {
	return fmt.Sprintf("%s_%d", str, time.Now().Nanosecond())
}

func createSQSQueue(t *testing.T, sqsService *sqs.SQS) (*sqs.CreateQueueOutput, func()) {
	qName := uniqueString("test-outbox-sqs")
	out, err := sqsService.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		t.Error(err)
	}
	cleaner := func() {
		_, err := sqsService.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
		if err != nil {
			t.Error(err)
		}
	}

	return out, cleaner
}
