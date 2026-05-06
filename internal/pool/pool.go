package pool

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/scheduler"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
)

// Pool manages a fixed number of goroutines that process jobs from a queue.
type Pool struct {
	workers   int
	queue     *queue.Queue
	scheduler *scheduler.Scheduler
	store     store.Store
	wg        sync.WaitGroup
}

// New creates a new Pool.
func New(workers int, q *queue.Queue, sched *scheduler.Scheduler, s store.Store) *Pool {
	return &Pool{
		workers:   workers,
		queue:     q,
		scheduler: sched,
		store:     s,
	}
}

// Start spawns `workers` goroutines that each pull jobs from the queue.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case job := <-p.queue.Drain():
					p.processJob(ctx, job)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

// Stop waits for all workers to finish after the context is cancelled.
func (p *Pool) Stop() {
	p.wg.Wait()
}

func (p *Pool) processJob(ctx context.Context, job *queue.Job) {
	log.Printf("[pool] worker processing run: %s", job.RunID)
	start := time.Now()

	defer func() {
		job.LogWriter.Close()
		log.Printf("[pool] run %s completed in %s", job.RunID, time.Since(start).Round(time.Millisecond))
	}()

	if err := p.scheduler.RunPipeline(ctx, job.Pipeline, job.RunID, job.LogWriter); err != nil {
		log.Printf("[pool] run %s failed: %v", job.RunID, err)
		_ = p.store.UpdateRunStatus(ctx, job.RunID, drevtypes.StatusFailed)
	}
}
