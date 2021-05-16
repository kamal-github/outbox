package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	outbox_worker "github.com/kamal-github/outbox"
	"github.com/kamal-github/outbox/backend"
	"github.com/kamal-github/outbox/datastore"
	"github.com/kamal-github/outbox/event"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestOutbox_SQSWithPostgres_success(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()

	// SQS setup
	sqsConn := setupSQS(t)
	createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
	defer queueCleaner()

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		expectedMessages[i] = uniqueString("HelloSQS")
	}

	// DB setup
	outboxTable := uniqueString("outbox")
	db, dbCleaner := setupPostgres(t, outboxTable)
	defer dbCleaner()

	for _, m := range expectedMessages {
		populateOutboxTable(t, db, outboxTable, []event.OutboxRow{
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
	pg, err := datastore.NewPostgres(db, outboxTable, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Dispatch
	simpleQueueService, err := backend.NewSimpleQueueService(sqsConn, pg, logger)
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
	rows, err := getRowsFromOutboxTable(db, outboxTable)
	if err != nil {
		t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
	}

	if len(rows) != 0 {
		t.Errorf("rows expected=%d, got=%d", 0, len(rows))
	}

	cancel()
	<-done
}

func TestOutbox_SQSWithPostgres_withBadData(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()

	// SQS setup
	sqsConn := setupSQS(t)
	createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
	defer queueCleaner()

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		expectedMessages[i] = uniqueString("HelloSQS")
	}

	params := make([]SQSQueueParam, len(expectedMessages))
	for i, m := range expectedMessages {
		params[i] = SQSQueueParam{
			QueueURL: createQueueOutput.QueueUrl,
			Body:     aws.String(m),
		}
	}

	// DB setup
	outboxTable := uniqueString("outbox")
	db, dbCleaner := setupPostgres(t, outboxTable)
	defer dbCleaner()

	for _, m := range expectedMessages {
		populateOutboxTable(t, db, outboxTable, []event.OutboxRow{
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
	populateOutboxTable(t, db, outboxTable, []event.OutboxRow{
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
	pg, err := datastore.NewPostgres(db, outboxTable, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Dispatch
	simpleQueueService, err := backend.NewSimpleQueueService(sqsConn, pg, logger)
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
	rows, err := getRowsFromOutboxTable(db, outboxTable)
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

var (
	jsonType = "application/json"
	amqpURL  = os.Getenv("BACKEND_URL")
)

func TestOutbox_RabbitMQWithPostgres_withSuccess(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	tests := []struct {
		name              string
		events            func() []event.OutboxRow
		expectedRowsCount int
	}{
		{
			name: "it successfully publishes and remove the rows from DB",
			events: func() []event.OutboxRow {
				qn, ex, rKey := uniqueString("test.outbox.queue"), uniqueString("test.outbox.exchange"), uniqueString("test.outbox.rKey")
				_, closer := setup(t, qn, ex, rKey)
				defer closer()

				rows := make([]event.OutboxRow, 5)
				for i := 0; i < 5; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							RabbitCfg: &event.RabbitCfg{
								Exchange:   ex,
								RoutingKey: rKey,
								Mandatory:  false,
								Immediate:  false,
								Publishing: amqp.Publishing{
									ContentType:  jsonType,
									Timestamp:    time.Now().UTC(),
									Body:         []byte(uniqueString("Hello from Outbox")),
									DeliveryMode: amqp.Transient,
								},
							},
						},
					}
				}
				return rows
			},
			expectedRowsCount: 0,
		},
		{
			name: "it keeps the rows in `InProcess` status for rows with invalid Metadata",
			events: func() []event.OutboxRow {
				rows := make([]event.OutboxRow, 3)
				for i := 0; i < 3; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							RabbitCfg: nil,
						},
					}
				}
				return rows
			},
			expectedRowsCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// DB setup
			outboxTable := uniqueString("outbox")
			db, dbCleaner := setupPostgres(t, outboxTable)
			defer dbCleaner()

			// Minesweeper
			pg, err := datastore.NewPostgres(db, outboxTable, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}

			// Dispatcher
			rabbitMQ, err := backend.NewRabbitMQ(
				amqpURL,
				pg,
				logger,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer rabbitMQ.Close()

			populateOutboxTable(t, db, outboxTable, tt.events())

			// Worker
			w := outbox_worker.Worker{
				MineSweeper:  pg,
				Dispatcher:   rabbitMQ,
				Logger:       logger,
				MineInterval: 1 * time.Millisecond,
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}, 1)
			defer cancel()

			go w.Start(ctx, done)

			time.Sleep(100 * time.Millisecond)

			cancel()
			<-done
			// assert outbox table count to be zero.
			rows, err := getRowsFromOutboxTable(db, outboxTable)
			if err != nil {
				t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
			}

			if len(rows) != tt.expectedRowsCount {
				t.Errorf("rows expected=%d, got=%d", 0, len(rows))
			}
		})
	}
}

func TestOutbox_RabbitMQWithPostgres_withBadData(t *testing.T) {
	logger := zap.NewNop()

	qn, ex, rKey := "test.outbox.queue", "test.outbox.exchange", "test.outbox.rKey"
	_, closer := setup(t, qn, ex, rKey)
	defer closer()

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		expectedMessages[i] = uniqueString("HelloRabbitMQ")
	}

	// DB setup
	outboxTable := uniqueString("outbox")
	db, dbCleaner := setupPostgres(t, outboxTable)
	defer dbCleaner()

	for _, m := range expectedMessages {
		populateOutboxTable(t, db, outboxTable, []event.OutboxRow{
			{
				Metadata: event.Metadata{
					RabbitCfg: &event.RabbitCfg{
						Exchange:   ex,
						RoutingKey: rKey,
						Mandatory:  false,
						Immediate:  false,
						Publishing: amqp.Publishing{
							ContentType:  jsonType,
							Timestamp:    time.Now().UTC(),
							Body:         []byte(m),
							DeliveryMode: amqp.Transient,
						},
					},
				},
				Payload: []byte(m),
			},
		})
	}

	// Minesweeper
	pg, err := datastore.NewPostgres(db, outboxTable, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Dispatcher
	rabbitMQ, err := backend.NewRabbitMQ(
		amqpURL,
		pg,
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rabbitMQ.Close()

	// Worker
	w := outbox_worker.Worker{
		MineSweeper:  pg,
		Dispatcher:   rabbitMQ,
		Logger:       logger,
		MineInterval: 1 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)
	defer cancel()

	go w.Start(ctx, done)

	time.Sleep(100 * time.Millisecond)

	// assert outbox table count to be zero.
	rows, err := getRowsFromOutboxTable(db, outboxTable)
	if err != nil {
		t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
	}

	if len(rows) != 0 {
		t.Errorf("rows expected=%d, got=%d", 0, len(rows))
	}

	cancel()
	<-done
}

func newConnection(t testing.TB) *amqp.Connection {
	t.Helper()

	conn, err := amqp.Dial(getURLFromEnv(t))
	assert.NoError(t, err)

	return conn
}

func getURLFromEnv(tb testing.TB) string {
	tb.Helper()
	return os.Getenv("BACKEND_URL")
}

func setup(t testing.TB, qn, ex, rKey string) (*amqp.Channel, func() error) {
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

type SQSQueueParam struct {
	QueueURL *string
	Body     *string
}

func setupSQS(t *testing.T) *sqs.SQS {
	t.Helper()

	sqsHost := os.Getenv("SQS_HOST")

	// Create the SQS queue
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigDisable,
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials("test", "test", ""),
			Endpoint:    aws.String(sqsHost),
			Region:      aws.String("eu-central-1"),
		},
	}))

	return sqs.New(sess, aws.NewConfig().WithEndpoint(sqsHost).WithRegion("eu-central-1"))
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

func setupPostgres(t *testing.T, outboxTable string) (*sql.DB, func()) {
	// Create the outbox table

	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		t.Fatal(err)
	}
	dropTableIfExists := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, outboxTable)
	_, err = db.ExecContext(context.TODO(), dropTableIfExists)
	if err != nil {
		t.Fatal(err)
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s(
id SERIAL,
metadata bytea,
payload bytea,
status integer,
created_at timestamp,
deleted_at timestamp)
`, outboxTable)
	_, err = db.ExecContext(context.TODO(), createTable)
	if err != nil {
		t.Fatal(err)
	}

	return db, func() {
		_, err := db.Exec(fmt.Sprintf(`DROP TABLE %s;`, outboxTable))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	}
}

func populateOutboxTable(t *testing.T, db *sql.DB, outboxTable string, rows []event.OutboxRow) {
	t.Helper()

	insertQuery := fmt.Sprintf(`INSERT INTO %s(metadata, payload) VALUES($1,$2)`, outboxTable)
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

func getRowsFromOutboxTable(db *sql.DB, outboxTable string) ([]event.OutboxRow, error) {
	q := fmt.Sprintf(`select id, metadata, payload, status from %s`, outboxTable)
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
