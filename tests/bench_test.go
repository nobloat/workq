package tests

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nobloat/workq"
)

// BenchmarkThroughput measures raw enqueue and execution rates of no-op tasks.
// Each op is one task: p producers enqueue b.N tasks, then an executor with p workers runs them.
func BenchmarkThroughput(b *testing.B) {
	for _, p := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("sqlite/p=%d", p), func(b *testing.B) { benchThroughput[*sql.Tx](b, setupSQLite(b), p) })
		b.Run(fmt.Sprintf("postgres/p=%d", p), func(b *testing.B) { benchThroughput[*sql.Tx](b, setupPostgres(b), p) })
		b.Run(fmt.Sprintf("pgx/p=%d", p), func(b *testing.B) { benchThroughput[pgx.Tx](b, setupPgx(b), p) })
	}
}

func benchThroughput[T any](b *testing.B, e env[T], p int) {
	ctx := context.Background()
	q := e.open(b, p+2).queue("", p) // workers + poll loop + heartbeat
	q.PollInterval = 10 * time.Millisecond
	q.Register("noop", func(context.Context, string) error { return nil })
	b.ResetTimer()

	begin := time.Now()
	var wg sync.WaitGroup
	var n atomic.Int64
	for range p {
		wg.Go(func() {
			for n.Add(1) <= int64(b.N) {
				if _, err := q.Enqueue(ctx, workq.Task{Name: "noop"}); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	enqueued := time.Since(begin)

	begin = time.Now()
	q.Start(ctx)
	if err := q.Wait(ctx); err != nil {
		b.Fatal(err)
	}
	executed := time.Since(begin)
	b.StopTimer()
	if err := q.Stop(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/enqueued.Seconds(), "enqueue/s")
	b.ReportMetric(float64(b.N)/executed.Seconds(), "exec/s")
}
