package tests

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobloat/workq"
)

// recordDriver is faultDriver that also records the name argument of every
// schedule insert, in execution order.
type recordDriver struct {
	faultDriver
	mu    *sync.Mutex
	names *[]string
}

type recordConn struct {
	faultConn
	mu    *sync.Mutex
	names *[]string
}

func (d recordDriver) DB() workq.Conn {
	return recordConn{faultConn{d.db, d.hook}, d.mu, d.names}
}

func (d recordDriver) Tx(tx *sql.Tx) workq.Conn {
	return recordConn{faultConn{tx, d.hook}, d.mu, d.names}
}

func (c recordConn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "scheduled") && len(args) > 0 {
		c.mu.Lock()
		*c.names = append(*c.names, fmt.Sprint(args[0]))
		c.mu.Unlock()
	}
	return c.faultConn.Exec(ctx, query, args...)
}

// TestSchedulesCreatedInNameOrder: Start creates the schedule rows in name
// order, not in map iteration order. Two nodes starting at the same time
// insert the same rows in one transaction each; with different orders, each
// waits for a row the other inserted, and Postgres aborts one of them with
// "deadlock detected" (SQLSTATE 40P01). With the same order, the second
// transaction just waits for the first.
func TestSchedulesCreatedInNameOrder(t *testing.T) {
	e := setupSQLite(t)
	var names []string
	d := recordDriver{faultDriver{e.db, func(string) error { return nil }}, &sync.Mutex{}, &names}
	q := workq.New[*sql.Tx](d, "", 1, slog.Default())
	q.PollInterval = time.Second
	var want []string
	for i := 19; i >= 0; i-- {
		name := fmt.Sprintf("task-%02d", i)
		want = append(want, name)
		if err := q.Register(name, func(context.Context, string) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := q.Schedule(name, workq.Every(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		q.Stop(ctx)
	})
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("schedules created in order %v, want %v", names, want)
	}
}
