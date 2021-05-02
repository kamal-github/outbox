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

type Postgres struct {
	db     *sql.DB
	table  string
	logger *zap.Logger
}

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

func (p Postgres) Close() error {
	return p.db.Close()
}

func (p Postgres) Mine(ctx context.Context) ([]event.OutboxRow, error) {
	q := fmt.Sprintf(
		"select id, metadata, payload from %s where status IS NULL", p.table,
	)

	rows, err := p.db.QueryContext(ctx, q)
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

func (p Postgres) Sweep(ctx context.Context, relayedIDs []int, failedIDs []int) error {
	if err := p.onSuccess(ctx, relayedIDs); err != nil {
		return err
	}
	if err := p.onFailure(ctx, failedIDs); err != nil {
		return err
	}

	return nil
}

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
