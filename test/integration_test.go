package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/kamal-github/outbox/event"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	outbox_worker "github.com/kamal-github/outbox"
	"github.com/kamal-github/outbox/backend"
	"github.com/kamal-github/outbox/datastore"
	"go.uber.org/zap"
)

func TestOutboxSQSWithPostgres_success(t *testing.T) {
	logger := zap.NewNop()

	// SQS setup
	sqsConn := setupSQS(t)
	createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
	defer queueCleaner()

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		expectedMessages[i] = fmt.Sprintf("HelloSQS-%d", unique())
	}

	// DB setup
	db, dbCleaner := setupPostgres(t)
	defer dbCleaner()

	for _, m := range expectedMessages {
		populateOutboxTable(t, db, []event.OutboxRow{
			{
				Metadata: event.Metadata{
					SQSCfg: &event.SQSCfg{
						&sqs.SendMessageInput{
							MessageBody: aws.String(m),
							QueueUrl:    createQueueOutput.QueueUrl,
						},
					},
				},
			},
		})
	}

	// Minesweeper
	pg, err := datastore.NewPostgres(db, "outbox", zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Dispatch
	simpleQueueService, err := backend.NewSimpleQueueService(sqsConn, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Worker
	w := outbox_worker.Worker{
		MineSweeper:  pg,
		Dispatcher:   simpleQueueService,
		Logger:       logger,
		MineInterval: 1 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)
	defer cancel()

	go w.Start(ctx, done)

	// assert the consumption of messages from SQS Queue
	// for count and message body.
	for i := 0; i < len(expectedMessages); i++ {
		recOut, err := sqsConn.ReceiveMessage(&sqs.ReceiveMessageInput{
			WaitTimeSeconds:     aws.Int64(2),
			MaxNumberOfMessages: aws.Int64(1),
			QueueUrl:            createQueueOutput.QueueUrl,
		})
		if err != nil {
			t.Errorf("expected=%v,got error=%v", nil, err)
		}
		if !contains(t, expectedMessages, *recOut.Messages[0].Body) {
			t.Errorf("expected=%v,got=%v", "HelloSQS", *recOut.Messages[0].Body)
		}
	}

	// assert outbox table count to be zero.
	rows, err := getRowsFromOutboxTable(db)
	if err != nil {
		t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
	}

	if len(rows) != 0 {
		t.Errorf("rows expected=%d, got=%d", 0, len(rows))
	}

	cancel()
	<-done
}

func TestOutboxSQSWithPostgres_withBadData(t *testing.T) {
	logger := zap.NewNop()

	// SQS setup
	sqsConn := setupSQS(t)
	createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
	defer queueCleaner()

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		expectedMessages[i] = fmt.Sprintf("HelloSQS-%d", unique())
	}

	params := make([]SQSQueueParam, len(expectedMessages))
	for i, m := range expectedMessages {
		params[i] = SQSQueueParam{
			QueueURL: createQueueOutput.QueueUrl,
			Body:     aws.String(m),
		}
	}

	// DB setup
	db, dbCleaner := setupPostgres(t)
	defer dbCleaner()

	for _, m := range expectedMessages {
		populateOutboxTable(t, db, []event.OutboxRow{
			{
				Metadata: event.Metadata{
					SQSCfg: &event.SQSCfg{
						&sqs.SendMessageInput{
							MessageBody: aws.String(m),
							QueueUrl:    createQueueOutput.QueueUrl,
						},
					},
				},
			},
		})
	}

	// adding intentional bad messages
	populateOutboxTable(t, db, []event.OutboxRow{
		{
			Metadata: event.Metadata{
				SQSCfg: nil,
			},
		},
		{
			// Empty queue URL
			Metadata: event.Metadata{
				SQSCfg: &event.SQSCfg{
					&sqs.SendMessageInput{
						MessageBody: aws.String("Corono Virus is gonna be over very soon!"),
					},
				},
			},
		},
	})

	// Minesweeper
	pg, err := datastore.NewPostgres(db, "outbox", zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Dispatch
	simpleQueueService, err := backend.NewSimpleQueueService(sqsConn, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Worker
	w := outbox_worker.Worker{
		MineSweeper:  pg,
		Dispatcher:   simpleQueueService,
		Logger:       logger,
		MineInterval: 1 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)
	defer cancel()

	go w.Start(ctx, done)

	// assert the consumption of messages from SQS Queue
	// for count and message body.
	for i := 0; i < len(expectedMessages); i++ {
		recOut, err := sqsConn.ReceiveMessage(&sqs.ReceiveMessageInput{
			WaitTimeSeconds:     aws.Int64(2),
			MaxNumberOfMessages: aws.Int64(1),
			QueueUrl:            createQueueOutput.QueueUrl,
		})
		if err != nil {
			t.Errorf("expected=%v,got error=%v", nil, err)
		}
		if !contains(t, expectedMessages, *recOut.Messages[0].Body) {
			t.Errorf("expected=%v,got=%v", "HelloSQS", *recOut.Messages[0].Body)
		}
	}

	// assert outbox table count to be zero.
	rows, err := getRowsFromOutboxTable(db)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
		}

		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Errorf("rows expected=%d, got=%d", 2, len(rows))
	}
	for _, r := range rows {
		if r.Status.Int64 != int64(datastore.Failed) {
			t.Errorf("status expected=%v, got=%v", datastore.Failed, r.Status.Int64)
		}
	}

	cancel()
	<-done
}

//func TestOutboxRabbitMQWithPostgres(t *testing.T) {
//	logger := zap.NewNop()
//
//	// Rabbit setup
//	rabbitConn := setupRabbit(t)
//	createQueueOutput, queueCleaner := createRabbitQueue(t, rabbitConn)
//	defer queueCleaner()
//
//	expectedMessages := make([]string, 5)
//	for i := 0; i < 5; i++ {
//		expectedMessages[i] = fmt.Sprintf("HelloRabbitMQ-%d", unique())
//	}
//
//	// DB setup
//	db, dbCleaner := setupPostgres(t)
//	defer dbCleaner()
//
//	for _, m := range expectedMessages {
//		populateOutboxTable(t, db, []event.OutboxRow{
//			{
//				Metadata: event.Metadata{
//					RabbitCfg: &event.RabbitCfg{
//						Exchange:   "",
//						RoutingKey: "",
//						Mandatory:  false,
//						Immediate:  false,
//						Publishing: event.Publishing{},
//					},
//				},
//				Payload: []byte(m),
//			},
//		})
//	}
//
//	// Minesweeper
//	pg, err := datastore.NewMineSweeper(db, "postgres", "outbox", zap.NewNop())
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Dispatch
//	rabbitMQ, err := backend.Provider(
//		config.ENV{
//			Backend:    "rabbitmq",
//			BackendURL: "rabbitmq://localhost:5672",
//		},
//		nil,
//		logger,
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Worker
//	w := pkg.Worker{
//		MineSweeper:    pg,
//		Dispatcher: rabbitMQ,
//		Logger:       logger,
//		MineInterval: 1 * time.Millisecond,
//	}
//
//	ctx, cancel := context.WithCancel(context.Background())
//	done := make(chan struct{}, 1)
//	defer cancel()
//
//	go w.Start(ctx, done)
//
//	// assert the consumption of messages for count and message body from RabbitMQ Queue.
//	for i := 0; i < len(expectedMessages); i++ {
//	}
//
//	// assert outbox table count to be zero.
//	rows, err := getRowsFromOutboxTable(db)
//	if err != nil {
//		t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
//	}
//
//	if len(rows) != 0 {
//		t.Errorf("rows expected=%d, got=%d", 0, len(rows))
//	}
//
//	cancel()
//	<-done
//}

type SQSQueueParam struct {
	QueueURL *string
	Body     *string
}

func setupSQS(t *testing.T) *sqs.SQS {
	t.Helper()

	// Create the SQS queue
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigDisable,
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials("test", "test", ""),
			Endpoint:    aws.String("http://localhost:4566"),
			Region:      aws.String("eu-central-1"),
		},
	}))

	return sqs.New(sess, aws.NewConfig().WithEndpoint("http://localhost:4566").WithRegion("eu-central-1"))
}

func unique() int {
	return time.Now().Nanosecond()
}

func createSQSQueue(t *testing.T, sqsService *sqs.SQS) (*sqs.CreateQueueOutput, func()) {
	qName := fmt.Sprintf("%s-%d", "test-oubox-sqs", unique())
	out, err := sqsService.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		t.Fatal(err)
	}
	cleaner := func() {
		_, err := sqsService.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
		if err != nil {
			t.Fatal(err)
		}
	}

	return out, cleaner
}

func setupPostgres(t *testing.T) (*sql.DB, func()) {
	// Create the outbox table
	db, err := sql.Open("postgres", "postgres://postgres:password@localhost:5432/test-outbox?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	dropTableIfExists := `DROP TABLE IF EXISTS outbox;`
	_, err = db.ExecContext(context.TODO(), dropTableIfExists)
	if err != nil {
		t.Fatal(err)
	}

	createTable := `CREATE TABLE outbox(
id SERIAL,
metadata bytea,
payload bytea,
status integer,
created_at timestamp,
deleted_at timestamp);
`
	_, err = db.ExecContext(context.TODO(), createTable)
	if err != nil {
		t.Fatal(err)
	}

	return db, func() {
		_, err := db.Exec(`DROP TABLE OUTBOX;`)
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	}
}

func populateOutboxTable(t *testing.T, db *sql.DB, rows []event.OutboxRow) {
	t.Helper()

	insertQuery := `INSERT INTO outbox(metadata, payload) VALUES($1,$2)`
	preparedStmt, err := db.Prepare(insertQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer preparedStmt.Close()

	for _, row := range rows {
		_, err = preparedStmt.Exec(&row.Metadata, &row.Payload)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func getRowsFromOutboxTable(db *sql.DB) ([]event.OutboxRow, error) {
	q := `select id, metadata, payload, status from outbox`
	var (
		outboxRows []event.OutboxRow
		err        error
	)

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var or event.OutboxRow
		if err := rows.Scan(&or.OutboxID, &or.Metadata, &or.Payload, &or.Status); err != nil {
			return nil, err
		}

		outboxRows = append(outboxRows, or)
	}

	return outboxRows, nil
}

func contains(t *testing.T, strs []string, str string) bool {
	t.Helper()

	for _, s := range strs {
		if s == str {
			return true
		}
	}

	return false
}
