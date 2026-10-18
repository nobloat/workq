package testing_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/nobloat/workq"
	_ "modernc.org/sqlite"
)

func TestEnqueuedTask(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "workq.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	q := workq.NewSQLite(db, "", 1, nil)
	if err := q.CreateTable(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	if err := q.Register("send-email", func(_ context.Context, param string) error {
		got <- param
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := q.Stop(ctx); err != nil {
			t.Error(err)
		}
	})

	if _, err := q.Enqueue(t.Context(), workq.Task{Name: "send-email", Param: "a@b.c"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Wait(t.Context(), "send-email"); err != nil {
		t.Fatal(err)
	}
	if actual := <-got; actual != "a@b.c" {
		t.Fatalf("sent to %q", actual)
	}
}
