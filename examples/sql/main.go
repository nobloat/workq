// sql runs one PostgreSQL queue through database/sql.
package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nobloat/workq"
)

func main() {
	ctx := context.Background()
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3) // one worker, poll loop, heartbeat
	db.SetMaxIdleConns(3)

	q := workq.NewPostgres(db, "", 1, slog.Default())
	if err := q.CreateTable(ctx); err != nil {
		log.Fatal(err)
	}
	if err := q.Register("hello", func(_ context.Context, param string) error {
		log.Printf("hello %s", param)
		return nil
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

func stop(q *workq.Queue[*sql.Tx]) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := q.Stop(ctx); err != nil {
		log.Print(err)
	}
}
