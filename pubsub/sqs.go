package pubsub

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

// SimpleQueueService represents the SimpleQueueService specific Dispatcher.
type SimpleQueueService struct {
	sqsConn *sqs.SQS
	sweeper Sweeper

	logger *zap.Logger
}

// Dispatch relays the message to SQS queue.
// Successful publish is considered soon after SendMessageWithContext returns no error, and then sweeper sweeps the
// successful outbox rows.
//
// Failed to publish are immediately marked as "Failed" by sweeper.
func (s *SimpleQueueService) Dispatch(ctx context.Context, rows []event.OutboxRow) (err error) {
	var mi *sqs.SendMessageInput

	var failedIDs, dispatchedIDs []int

	for _, row := range rows {
		if row.Metadata.SQSCfg == nil {
			s.logger.Error("invalid SQS config")
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		mi = row.Metadata.SQSCfg.SendMessageInput
		if _, err = s.sqsConn.SendMessageWithContext(ctx, mi); err != nil {
			s.logger.Error("failed to send message to SQS queue", zap.Error(err))
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		dispatchedIDs = append(dispatchedIDs, row.OutboxID)
	}

	if err = s.sweeper.Sweep(ctx, dispatchedIDs, failedIDs); err != nil {
		return
	}

	s.logger.Debug("Messages relayed", zap.Ints("dispatchedIDs", dispatchedIDs), zap.Ints("failedIDs", failedIDs))
	return
}

// Close Not implemented yet. No documentation found for
// closing AWS session.
func (s *SimpleQueueService) Close() error {
	return errors.New("no method on SQS connection")
}

// NewSimpleQueueService creates a SimpleQueueService dispatcher.
func NewSimpleQueueService(sqsConn *sqs.SQS, sw Sweeper, logger *zap.Logger) (*SimpleQueueService, error) {
	if sqsConn == nil {
		return nil, errors.New("SimpleQueueService: invalid SQS connection")
	}
	if logger == nil {
		return nil, errors.New("SimpleQueueService: invalid logger")
	}

	return &SimpleQueueService{
		sqsConn: sqsConn,
		logger:  logger,
		sweeper: sw,
	}, nil
}
