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
	logger  *zap.Logger
}

func NewSimpleQueueService(sqsConn *sqs.SQS, logger *zap.Logger) (Dispatcher, error) {
	if sqsConn == nil {
		return nil, errors.New("SimpleQueueService: invalid SQS connection")
	}
	if logger == nil {
		return nil, errors.New("SimpleQueueService: invalid logger")
	}

	return &SimpleQueueService{
		sqsConn: sqsConn,
		logger:  logger,
	}, nil
}

func (s *SimpleQueueService) Dispatch(ctx context.Context, rows []event.OutboxRow) ([]int, []int, error) {
	successIDs := make([]int, 0)
	failedIDs := make([]int, 0)

	var (
		mi  *sqs.SendMessageInput
		err error
	)

	for _, row := range rows {
		if row.Metadata.SQSCfg == nil {
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		mi = row.Metadata.SQSCfg.SendMessageInput
		if _, err = s.sqsConn.SendMessageWithContext(ctx, mi); err != nil {
			s.logger.Error("failed to send message to SQS queue", zap.Error(err))
			failedIDs = append(failedIDs, row.OutboxID)
			continue
		}

		successIDs = append(successIDs, row.OutboxID)
	}

	s.logger.Debug("Messages relayed", zap.Ints("successIDs", successIDs), zap.Ints("failedIDs", failedIDs))
	return successIDs, failedIDs, nil
}

func (s *SimpleQueueService) Close() error {
	// Not implemented yet. No documentation found for
	// closing AWS session.
	return nil
}
