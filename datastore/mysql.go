package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kamal-github/outbox/event"
	"go.uber.org/zap"
)

type MySQL struct {
	db     *sql.DB
	table  string
	logger *zap.Logger
}

func NewMySQL(db *sql.DB, table string, logger *zap.Logger) (MineSweeper, error) {
	if db == nil {
		return MySQL{}, fmt.Errorf("%s: %w", "NewMySQL", errors.New("nil DB"))
	}

	return MySQL{
		db:     db,
		table:  table,
		logger: logger,
	}, nil
}

func (p MySQL) Mine(ctx context.Context) (outboxRows []event.OutboxRow, err error) {
	selectQuery := fmt.Sprintf(`SELECT id, metadata, payload FROM %s WHERE status IS NULL FOR UPDATE`, p.table)
	updateQuery := fmt.Sprintf(
		`UPDATE %s SET status=? where STATUS IS NULL`, p.table,
	)

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, selectQuery)
	if err == sql.ErrNoRows {
		return nil, ErrNoEvents
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	_, err = tx.ExecContext(ctx, updateQuery, Failed)
	if err != nil {
		return nil, err
	}

	tx.Commit()

	for rows.Next() {
		var or event.OutboxRow
		if err := rows.Scan(&or.OutboxID, &or.Metadata, &or.Payload); err != nil {
			p.logger.Error("error while scan", zap.Error(err))
			return nil, err
		}

		outboxRows = append(outboxRows, or)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return outboxRows, nil
}

func (p MySQL) Sweep(ctx context.Context, relayedIDs []int, failedIDs []int) error {
	if err := p.onSuccess(ctx, relayedIDs); err != nil {
		return err
	}
	if err := p.onFailure(ctx, failedIDs); err != nil {
		return err
	}

	return nil
}

func (p MySQL) Close() error {
	return p.db.Close()
}

func (p MySQL) onSuccess(ctx context.Context, ids []int) error {
	if len(ids) == 0 {
		return nil
	}

	const errPrefix = "MySQL.onSuccess"

	q := fmt.Sprintf(
		`DELETE FROM %s WHERE id IN (%s)`, p.table, buildPlaceholders(len(ids)),
	)

	res, err := p.db.ExecContext(ctx, q, mapIntSliceInterfaceSlice(ids)...)
	if err != nil {
		return fmt.Errorf("%s: error while deleting records: %w", errPrefix, err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != int64(len(ids)) {
		return fmt.Errorf("%s: records were not deleted partially/completely: %w", errPrefix, err)
	}

	p.logger.Info("outbox rows deleted", zap.Ints("outboxIDs", ids))

	return nil
}

func (p MySQL) onFailure(ctx context.Context, ids []int) error {
	if len(ids) == 0 {
		return nil
	}

	const errPrefix = "MySQL.onFailure"

	q := fmt.Sprintf(
		`UPDATE %s SET STATUS=? WHERE id IN (%s)`,
		p.table,
		buildPlaceholders(len(ids)),
	)

	args := make([]interface{}, 0)
	args = append(args, Failed)
	args = append(args, mapIntSliceInterfaceSlice(ids)...)
	res, err := p.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("%s: error while setting failed status: %w", errPrefix, err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != int64(len(ids)) {
		return fmt.Errorf("%s: Status=Failed was not updated partially/completely: %w", errPrefix, err)
	}

	p.logger.Info("outbox rows marked as failed", zap.Ints("outboxIDs", ids))

	return nil
}

func buildPlaceholders(n int) string {
	return "?" + strings.Repeat(",?", n-1)
}

func mapIntSliceInterfaceSlice(integers []int) []interface{} {
	var iFaces []interface{}

	for _, integer := range integers {
		iFaces = append(iFaces, integer)
	}

	return iFaces
}
