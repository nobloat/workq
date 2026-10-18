// queues runs independent worker pools for the default and "mails" queues.
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

	orders := workq.NewPostgres(db, "", 4, slog.Default())
	mails := workq.NewPostgres(db, "mails", 1, slog.Default())
	if err := orders.CreateTable(ctx); err != nil {
		log.Fatal(err)
	}
	if err := orders.Register("charge-order", printTask); err != nil {
		log.Fatal(err)
	}
	if err := mails.Register("send-email", printTask); err != nil {
		log.Fatal(err)
	}
	if err := orders.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer stop(orders)
	if err := mails.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer stop(mails)

	if _, err := orders.Enqueue(ctx, workq.Task{Name: "charge-order", Param: "order-1"}); err != nil {
		log.Fatal(err)
	}
	if _, err := orders.Enqueue(ctx, workq.Task{Name: "send-email", Param: "order-1", Queue: "mails"}); err != nil {
		log.Fatal(err)
	}
	if err := orders.Wait(ctx); err != nil {
		log.Fatal(err)
	}
	if err := mails.Wait(ctx); err != nil {
		log.Fatal(err)
	}
}

func printTask(_ context.Context, param string) error {
	log.Printf("processing %s", param)
	return nil
}

func stop(q *workq.Queue[*sql.Tx]) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := q.Stop(ctx); err != nil {
		log.Print(err)
	}
}
