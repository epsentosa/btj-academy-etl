package lib

import (
	"context"
	"fmt"
	"strings"

	"processor/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const tempTableSql = `
create temporary table %s (
	LIKE %s INCLUDING ALL
)
ON COMMIT DROP;
`

const insertTempTableSql = `
WITH ins AS (
  INSERT INTO %s
  SELECT * FROM %s
  RETURNING *
)
SELECT COUNT(*) FROM ins;
`

const insertTempTableSqlWithUniqueConstraint = `
WITH ins AS (
  INSERT INTO %s
  SELECT * FROM %s
  ON CONFLICT (%s)
  DO UPDATE SET
  	%s
  RETURNING *
)
SELECT COUNT(*) FROM ins;
`

type TableInsert struct {
	name     string
	column   []string
	rowBatch [][]any
	// colIndex []string
	// colSet   []string
}

func NewTableInsert(tableName string, column []string, rowBatch [][]any) *TableInsert {
	return &TableInsert{
		name:     tableName,
		column:   column,
		rowBatch: rowBatch,
		// colIndex: colIndex,
		// colSet:   colSet,
	}
}

func NewDBConn(ctx context.Context, url string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(strings.TrimSpace(url))
	if err != nil {
		return nil, errors.Wrap(err, "pgx failed parsed url")
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "pgx failed connecting")
	}
	if err = conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	logger.Info("Database connection established")
	return conn, nil
}

func InsertUpdateDuplicateBatch(ctx context.Context, tbl *TableInsert, conn *pgxpool.Pool) (int64, error) {
	var (
		totalInserted int64
		tempTableName string
		// uniqueColumns string
		// setString     string
		// setColumns    string
		err error
	)

	tempTableName = fmt.Sprintf("%s_temp", tbl.name)

	// uniqueColumns = strings.Join(tbl.colIndex, ",")
	// for _, col := range tbl.colSet {
	// 	setString += fmt.Sprintf("%s = EXCLUDED.%s, ", col, col)
	// }
	// setColumns = strings.TrimSuffix(setString, ", ")

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed starting transaction")
	}
	defer func() {
		if err != nil {
			err = tx.Rollback(ctx)
			if err != nil {
				logger.Error("failed rolling back transaction", zap.Error(err))
			}
		} else {
			err = tx.Commit(ctx)
			if err != nil {
				logger.Error("failed committing transaction", zap.Error(err))
			}
		}
	}()

	query := fmt.Sprintf(tempTableSql, tempTableName, tbl.name)
	_, err = tx.Exec(ctx, query)
	if err != nil {
		return 0, errors.Wrap(err, "failed creating temporary table")
	}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{tempTableName}, tbl.column, pgx.CopyFromRows(tbl.rowBatch))
	if err != nil {
		return 0, errors.Wrap(err, "failed inserting to temporary table")
	}
	// query = fmt.Sprintf(insertTempTableSqlWithUniqueConstraint, tbl.name, tempTableName, uniqueColumns, setColumns)
	query = fmt.Sprintf(insertTempTableSql, tbl.name, tempTableName)
	err = tx.QueryRow(ctx, query).Scan(&totalInserted)
	if err != nil {
		return 0, errors.Wrap(err, "failed inserting to table")
	}

	return totalInserted, nil
}
