// pgx runs one PostgreSQL queue through a native pgx pool.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nobloat/workq"
	"github.com/nobloat/workq/pgxq"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS greeting_log (message TEXT NOT NULL)`); err != nil {
		log.Fatal(err)
	}

	q := pgxq.New(pool, "", 1, slog.Default())
	if err := q.CreateTable(ctx); err != nil {
		log.Fatal(err)
	}
	if err := q.Register("hello", func(ctx context.Context, param string) error {
		_, err := pool.Exec(ctx, `INSERT INTO greeting_log (message) VALUES ($1)`, param)
		return err
	}); err != nil {
		log.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer stop(q)

	if _, err := q.Enqueue(ctx, workq.Task{Name: "hello", Param: "world"}); err != nil {
		log.Fatal(err)
	}
	if err := q.Wait(ctx); err != nil {
		log.Fatal(err)
	}
}

func stop(q *workq.Queue[pgx.Tx]) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := q.Stop(ctx); err != nil {
		log.Print(err)
	}
}
