package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	outbox "github.com/kamal-github/outbox"
	"github.com/kamal-github/outbox/backend"
	"github.com/kamal-github/outbox/datastore"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	// Setup log
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	// Setup AWS session and SQS connection
	awsSession := session.Must(session.NewSession())
	sqsConn := sqs.New(awsSession)

	// Setup SQS as backend
	dispatcher, err := backend.NewSimpleQueueService(sqsConn, logger)
	if err != nil {
		panic(err)
	}
	defer dispatcher.Close()

	// Connect to Postgres
	dsName := "postgres://postgres:password@localhost:5432/test-outbox?sslmode=disable"
	dbConn, err := connectToSQLDB("postgres", dsName)
	if err != nil {
		panic(err)
	}

	// Setup Postgres as Minesweeper
	mineSweeper, err := datastore.NewPostgres(dbConn, "outbox", logger)
	if err != nil {
		panic(err)
	}
	defer mineSweeper.Close()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerDone := make(chan struct{})

	go outbox.Worker{
		MineSweeper:  mineSweeper,
		Dispatcher:   dispatcher,
		Logger:       logger,
		MineInterval: 2 * time.Second,
	}.Start(ctx, workerDone)

	<-sig
	cancel()

	<-workerDone
}

func connectToSQLDB(driver, dsName string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsName)
	if err != nil {
		return db, err
	}

	return db, nil
}
