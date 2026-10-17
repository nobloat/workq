// Package pgxq runs workq on a native pgx pool, so RegisterTx handlers and EnqueueTx use pgx.Tx.
package pgxq

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nobloat/workq"
)

func New(pool *pgxpool.Pool, queue string, workers int, log *slog.Logger) *workq.Queue[pgx.Tx] {
	return workq.New[pgx.Tx](driver{pool}, queue, workers, log)
}

type driver struct{ pool *pgxpool.Pool }

func (d driver) DB() workq.Conn                                { return conn{d.pool} }
func (d driver) Tx(tx pgx.Tx) workq.Conn                       { return conn{tx} }
func (d driver) SQLite() bool                                  { return false }
func (d driver) Begin(ctx context.Context) (pgx.Tx, error)     { return d.pool.Begin(ctx) }
func (d driver) Commit(ctx context.Context, tx pgx.Tx) error   { return tx.Commit(ctx) }
func (d driver) Rollback(ctx context.Context, tx pgx.Tx) error { return tx.Rollback(ctx) }

type conn struct {
	c interface {
		Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	}
}

func (c conn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	tag, err := c.c.Exec(ctx, query, args...)
	return tag.RowsAffected(), err
}

func (c conn) Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error {
	rows, err := c.c.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := row(rows.Scan); err != nil {
			return err
		}
	}
	return rows.Err()
}
