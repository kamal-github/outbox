package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kamal-github/outbox"
	"github.com/kamal-github/outbox/datastore"
	"github.com/kamal-github/outbox/event"
	"github.com/kamal-github/outbox/internal/config"
	"github.com/kamal-github/outbox/pubsub"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
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

func TestOutbox_SQSWithPostgres(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	sqsConn := setupSQS(t)

	tests := []struct {
		name              string
		events            func() ([]event.OutboxRow, func())
		expectedRowsCount int
		outboxStatus      datastore.Status
	}{
		{
			name: "it successfully publishes and remove the rows from DB",
			events: func() ([]event.OutboxRow, func()) {
				createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
				rows := make([]event.OutboxRow, 5)
				for i := 0; i < 5; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							SQSCfg: &event.SQSCfg{
								SendMessageInput: &sqs.SendMessageInput{
									MessageBody: aws.String(uniqueString("Hello from Outbox")),
									QueueUrl:    createQueueOutput.QueueUrl,
								},
							},
						},
					}
				}

				return rows, queueCleaner
			},
			expectedRowsCount: 0,
		},
		{
			name: "it keeps the rows in `InProcess` status for rows with invalid Metadata",
			events: func() ([]event.OutboxRow, func()) {
				rows := make([]event.OutboxRow, 3)
				for i := 0; i < 3; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							SQSCfg: nil,
						},
					}
				}
				return rows, func() {}
			},
			expectedRowsCount: 3,
			outboxStatus:      datastore.Failed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// DB setup
			outboxTable := uniqueString("outbox")
			db, dbCleaner := setupPostgres(t, outboxTable)
			defer dbCleaner()

			events, queueCleaner := tt.events()
			defer queueCleaner()

			populateOutboxTable(t, db, outboxTable, events, DOLLAR)

			// Minesweeper
			pg, err := datastore.NewPostgres(db, outboxTable, zap.NewNop())
			if err != nil {
				t.Error(err)
			}

			// Dispatcher
			simpleQueueService, err := pubsub.NewSimpleQueueService(sqsConn, pg, logger)
			if err != nil {
				t.Error(err)
			}

			// Worker
			w := outbox.Worker{
				MineSweeper:  pg,
				Dispatcher:   simpleQueueService,
				Logger:       logger,
				MineInterval: 1 * time.Millisecond,
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}, 1)
			defer cancel()

			go w.Start(ctx, done)

			//assert the consumption of messages from SQS Queue for count
			for _, e := range events {
				var queueURL *string
				if e.Metadata.SQSCfg == nil {
					continue
				}
				queueURL = e.Metadata.SQSCfg.QueueUrl
				_, err := sqsConn.ReceiveMessage(&sqs.ReceiveMessageInput{
					MaxNumberOfMessages: aws.Int64(1),
					QueueUrl:            queueURL,
				})
				if err != nil {
					t.Errorf("receiveMessage: expected=%v,got error=%v", nil, err)
				}
			}

			time.Sleep(100 * time.Millisecond)

			// assert outbox table count to be zero.
			rows, err := getRowsFromOutboxTable(db, outboxTable)
			if err != nil {
				if err == sql.ErrNoRows {
					t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
				}

				t.Error(err)
			}
			if len(rows) != tt.expectedRowsCount {
				t.Errorf("rows expected=%d, got=%d", tt.expectedRowsCount, len(rows))
			}
			for _, r := range rows {
				if r.Status.Int64 != int64(tt.outboxStatus) {
					t.Errorf("status expected=%v, got=%v", tt.outboxStatus, r.Status.Int64)
				}
			}

			cancel()
			<-done

		})
	}
}

func TestOutbox_SQSWithMySQL(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	sqsConn := setupSQS(t)

	tests := []struct {
		name              string
		events            func() ([]event.OutboxRow, func())
		expectedRowsCount int
		outboxStatus      datastore.Status
	}{
		{
			name: "it successfully publishes and remove the rows from DB",
			events: func() ([]event.OutboxRow, func()) {
				createQueueOutput, queueCleaner := createSQSQueue(t, sqsConn)
				rows := make([]event.OutboxRow, 5)
				for i := 0; i < 5; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							SQSCfg: &event.SQSCfg{
								SendMessageInput: &sqs.SendMessageInput{
									MessageBody: aws.String(uniqueString("Hello from Outbox")),
									QueueUrl:    createQueueOutput.QueueUrl,
								},
							},
						},
					}
				}

				return rows, queueCleaner
			},
			expectedRowsCount: 0,
		},
		{
			name: "it keeps the rows in `InProcess` status for rows with invalid Metadata",
			events: func() ([]event.OutboxRow, func()) {
				rows := make([]event.OutboxRow, 3)
				for i := 0; i < 3; i++ {
					rows[i] = event.OutboxRow{
						Metadata: event.Metadata{
							SQSCfg: nil,
						},
					}
				}
				return rows, func() {}
			},
			expectedRowsCount: 3,
			outboxStatus:      datastore.Failed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// DB setup
			outboxTable := uniqueString("outbox")
			db, dbCleaner := setupMySQL(t, outboxTable)
			defer dbCleaner()

			events, queueCleaner := tt.events()
			defer queueCleaner()

			populateOutboxTable(t, db, outboxTable, events, QUESTION)

			// Minesweeper
			pg, err := datastore.NewMySQL(db, outboxTable, zap.NewNop())
			if err != nil {
				t.Error(err)
			}

			// Dispatcher
			simpleQueueService, err := pubsub.NewSimpleQueueService(sqsConn, pg, logger)
			if err != nil {
				t.Error(err)
			}

			// Worker
			w := outbox.Worker{
				MineSweeper:  pg,
				Dispatcher:   simpleQueueService,
				Logger:       logger,
				MineInterval: 1 * time.Millisecond,
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}, 1)
			defer cancel()

			go w.Start(ctx, done)

			//assert the consumption of messages from SQS Queue for count
			for _, e := range events {
				var queueURL *string
				if e.Metadata.SQSCfg == nil {
					continue
				}
				queueURL = e.Metadata.SQSCfg.QueueUrl
				_, err := sqsConn.ReceiveMessage(&sqs.ReceiveMessageInput{
					MaxNumberOfMessages: aws.Int64(1),
					QueueUrl:            queueURL,
				})
				if err != nil {
					t.Errorf("receiveMessage: expected=%v,got error=%v", nil, err)
				}
			}

			time.Sleep(100 * time.Millisecond)

			// assert outbox table count to be zero.
			rows, err := getRowsFromOutboxTable(db, outboxTable)
			if err != nil {
				if err == sql.ErrNoRows {
					t.Errorf("getRowsFromOutboxTable error expected=%v, got=%v", nil, err)
				}

				t.Error(err)
			}
			if len(rows) != tt.expectedRowsCount {
				t.Errorf("rows expected=%d, got=%d", tt.expectedRowsCount, len(rows))
			}
			for _, r := range rows {
				if r.Status.Int64 != int64(tt.outboxStatus) {
					t.Errorf("status expected=%v, got=%v", tt.outboxStatus, r.Status.Int64)
				}
			}

			cancel()
			<-done

		})
	}
}

var (
	jsonType = "application/json"
)

func TestOutbox_RabbitMQWithPostgres(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	tests := []struct {
		name              string
		events            func() []event.OutboxRow
		expectedRowsCount int
		outboxStatus      datastore.Status
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
			outboxStatus:      datastore.Failed,
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
				t.Error(err)
			}

			// Dispatcher
			rabbitMQ, err := pubsub.NewRabbitMQ(
				envCfg.RabbitMQURI,
				pg,
				logger,
			)
			if err != nil {
				t.Error(err)
			}
			defer rabbitMQ.Close()

			populateOutboxTable(t, db, outboxTable, tt.events(), DOLLAR)

			// Worker
			w := outbox.Worker{
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
			for _, r := range rows {
				if r.Status.Int64 != int64(tt.outboxStatus) {
					t.Errorf("status expected=%v, got=%v", tt.outboxStatus, r.Status.Int64)
				}
			}
		})
	}
}

func TestOutbox_RabbitMQWithMySQL(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	tests := []struct {
		name              string
		events            func() []event.OutboxRow
		expectedRowsCount int
		outboxStatus      datastore.Status
	}{
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
			outboxStatus:      datastore.Failed,
		},
		{
			name: "it successfully publishes and removes the rows from DB",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// DB setup
			outboxTable := uniqueString("outbox")
			db, dbCleaner := setupMySQL(t, outboxTable)
			defer dbCleaner()

			// Minesweeper
			mysql, err := datastore.NewMySQL(db, outboxTable, zap.NewNop())
			if err != nil {
				t.Error(err)
			}

			// Dispatcher
			rabbitMQ, err := pubsub.NewRabbitMQ(
				envCfg.RabbitMQURI,
				mysql,
				logger,
			)
			if err != nil {
				t.Error(err)
			}
			defer rabbitMQ.Close()

			populateOutboxTable(t, db, outboxTable, tt.events(), QUESTION)

			// Worker
			w := outbox.Worker{
				MineSweeper:  mysql,
				Dispatcher:   rabbitMQ,
				Logger:       logger,
				MineInterval: 1 * time.Millisecond,
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}, 1)
			defer cancel()

			go w.Start(ctx, done)

			time.Sleep(200 * time.Millisecond)

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
			for _, r := range rows {
				if r.Status.Int64 != int64(tt.outboxStatus) {
					t.Errorf("status expected=%v, got=%v", tt.outboxStatus, r.Status.Int64)
				}
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

type SQSQueueParam struct {
	QueueURL *string
	Body     *string
}

func setupSQS(t *testing.T) *sqs.SQS {
	t.Helper()

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

func setupPostgres(t *testing.T, outboxTable string) (*sql.DB, func()) {
	// Create the table for storing outbox rows
	db, err := sql.Open("postgres", envCfg.PostgresURI)
	if err != nil {
		t.Error(err)
	}
	dropTableIfExists := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, outboxTable)
	_, err = db.ExecContext(context.TODO(), dropTableIfExists)
	if err != nil {
		t.Error(err)
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
		t.Error(err)
	}

	return db, func() {
		_, err := db.Exec(fmt.Sprintf(`DROP TABLE %s;`, outboxTable))
		if err != nil {
			t.Error(err)
		}

		db.Close()
	}
}

func setupMySQL(t *testing.T, outboxTable string) (*sql.DB, func()) {
	db, err := sql.Open("mysql", envCfg.MySQLURI)
	if err != nil {
		t.Error(err)
	}

	dropTableIfExists := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, outboxTable)
	_, err = db.ExecContext(context.TODO(), dropTableIfExists)
	if err != nil {
		t.Error(err)
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s(
id SERIAL,
metadata blob,
payload blob,
status int,
created_at timestamp,
deleted_at timestamp)
`, outboxTable)
	_, err = db.ExecContext(context.TODO(), createTable)
	if err != nil {
		t.Error(err)
	}

	return db, func() {
		_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, outboxTable))
		if err != nil {
			t.Error(err)
		}

		db.Close()
	}
}

type placeholderType int

const (
	DOLLAR placeholderType = iota + 1
	QUESTION
)

func populateOutboxTable(t *testing.T, db *sql.DB, outboxTable string, rows []event.OutboxRow, pt placeholderType) {
	t.Helper()

	p := placeholders(pt)
	insertQuery := fmt.Sprintf(`INSERT INTO %s(metadata, payload) VALUES(%s)`, outboxTable, p)
	preparedStmt, err := db.Prepare(insertQuery)
	if err != nil {
		t.Error(err)
	}
	defer preparedStmt.Close()

	for _, row := range rows {
		_, err = preparedStmt.Exec(&row.Metadata, &row.Payload)
		if err != nil {
			t.Error(err)
		}
	}
}

func placeholders(p placeholderType) string {
	args := "?,?"
	if p == DOLLAR {
		args = "$1,$2"
	}
	return args
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
