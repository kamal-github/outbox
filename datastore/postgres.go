package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/kamal-github/outbox/event"

	"github.com/lib/pq"
	"go.uber.org/zap"
)

// Postgres represents postgres implementation for MineSweeper.
type Postgres struct {
	db     *sql.DB
	table  string
	logger *zap.Logger
}

// NewPostgres constructs Postgres with all dependencies.
func NewPostgres(db *sql.DB, table string, logger *zap.Logger) (MineSweeper, error) {
	if db == nil {
		return Postgres{}, fmt.Errorf("%s: %w", "NewPostgres", errors.New("nil DB"))
	}

	return Postgres{
		db:     db,
		table:  table,
		logger: logger,
	}, nil
}

// Close closes the Postgres DB connection.
func (p Postgres) Close() error {
	return p.db.Close()
}

// Mine fetches and returns all the outbox rows which are ready to publish(having Status IS NULL).
//
// Marks the fetched rows Status to InProcess so as to avoid same records being re-fetched in another
// iteration by Worker.
func (p Postgres) Mine(ctx context.Context) ([]event.OutboxRow, error) {
	q := fmt.Sprintf(
		`UPDATE %s SET status=$1 WHERE status IS NULL RETURNING id, metadata, payload`, p.table,
	)

	rows, err := p.db.QueryContext(ctx, q, InProcess)
	if err == sql.ErrNoRows {
		return nil, ErrNoEvents
	}
	if err != nil {
		return nil, err
	}

	outboxRows := make([]event.OutboxRow, 0)

	for rows.Next() {
		var or event.OutboxRow
		if err := rows.Scan(&or.OutboxID, &or.Metadata, &or.Payload); err != nil {
			p.logger.Error("error while scan", zap.Error(err))
			return nil, err
		}

		outboxRows = append(outboxRows, or)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return outboxRows, nil

}

// Sweep deletes the dispatched outbox rows when successfully published.
// otherwise marks those records as Failed when failed to publish.
//
// It should be called by Messaging system.
func (p Postgres) Sweep(ctx context.Context, relayedIDs []int, failedIDs []int) error {
	if err := p.onSuccess(ctx, relayedIDs); err != nil {
		return err
	}
	if err := p.onFailure(ctx, failedIDs); err != nil {
		return err
	}

	return nil
}

// onSuccess deletes the outbox rows.
func (p Postgres) onSuccess(ctx context.Context, ids []int) error {
	if len(ids) == 0 {
		return nil
	}

	const errPrefix = "postgres.onSuccess"

	q := fmt.Sprintf(
		`DELETE FROM %s WHERE id = ANY($1)`, p.table,
	)

	res, err := p.db.ExecContext(ctx, q, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("%s: error while deleting records: %w", errPrefix, err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != int64(len(ids)) {
		return fmt.Errorf("%s: records were not deleted partially/completely: %w", errPrefix, err)
	}

	p.logger.Info("outbox rows deleted", zap.Ints("outboxIDs", ids))

	return nil
}

// onFailure marks the outbox rows as Failed.
func (p Postgres) onFailure(ctx context.Context, ids []int) error {
	if len(ids) == 0 {
		return nil
	}

	const errPrefix = "postgres.onFailure"

	q := fmt.Sprintf(
		`UPDATE %s SET STATUS=$1 WHERE id = ANY($2)`,
		p.table,
	)

	res, err := p.db.ExecContext(ctx, q, Failed, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("%s: error while setting failed status: %w", errPrefix, err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != int64(len(ids)) {
		return fmt.Errorf("%s: Status=Failed was not updated partially/completely: %w", errPrefix, err)
	}

	p.logger.Info("outbox rows marked as failed", zap.Ints("outboxIDs", ids))

	return nil
}
