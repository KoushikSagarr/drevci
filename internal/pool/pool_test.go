package pool

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/pkg/drevtypes"
)

// nopWriteCloser is an io.WriteCloser that discards all writes.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func makeJob(id string) *queue.Job {
	return &queue.Job{
		RunID:      id,
		Pipeline:   &drevtypes.Pipeline{Name: id},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}
}

// testPool is a minimal pool that uses a plain function instead of the real scheduler/store.
type testPool struct {
	workers int
	queue   *queue.Queue
	fn      func(context.Context, *queue.Job)
	wg      sync.WaitGroup
}

func newTestPool(workers int, q *queue.Queue, fn func(context.Context, *queue.Job)) *testPool {
	return &testPool{workers: workers, queue: q, fn: fn}
}

func (p *testPool) start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case job := <-p.queue.Drain():
					p.fn(ctx, job)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (p *testPool) stop() {
	p.wg.Wait()
}

func TestPool_ProcessesJobs(t *testing.T) {
	q := queue.New(10)
	processed := &atomic.Int32{}

	tp := newTestPool(2, q, func(ctx context.Context, job *queue.Job) {
		time.Sleep(50 * time.Millisecond) // simulate work
		processed.Add(1)
		job.LogWriter.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	tp.start(ctx)

	for i := 0; i < 4; i++ {
		q.Enqueue(makeJob(string(rune('a' + i))))
	}

	deadline := time.After(5 * time.Second)
	for {
		if processed.Load() == 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: only %d/4 jobs completed", processed.Load())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	tp.stop()
}

func TestPool_CancelContext(t *testing.T) {
	q := queue.New(10)
	tp := newTestPool(2, q, func(ctx context.Context, job *queue.Job) {
		job.LogWriter.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	tp.start(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		tp.stop()
		close(done)
	}()

	select {
	case <-done:
		// pool stopped cleanly
	case <-time.After(3 * time.Second):
		t.Fatal("pool did not stop cleanly after context cancellation")
	}
}
