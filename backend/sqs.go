package backend

import (
	"context"
	"errors"

	"github.com/kamal-github/outbox/event"

	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"
)

type SimpleQueueService struct {
	sqsConn *sqs.SQS
	sweeper Sweeper

	logger *zap.Logger
}

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

func (s *SimpleQueueService) Dispatch(ctx context.Context, rows []event.OutboxRow) (err error) {
	var mi *sqs.SendMessageInput

	dispatchedIDs := make(DispatchedIDs, 0)
	failedIDs := make(FailedIDs, 0)

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

func (s *SimpleQueueService) Close() error {
	// Not implemented yet. No documentation found for
	// closing AWS session.
	return errors.New("no method on SQS connection")
}
