package pool

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/drevci/drev/internal/notify"
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
	notifier  *notify.Notifier
	wg        sync.WaitGroup
}

// New creates a new Pool.
func New(workers int, q *queue.Queue, sched *scheduler.Scheduler, s store.Store, notif *notify.Notifier) *Pool {
	return &Pool{
		workers:   workers,
		queue:     q,
		scheduler: sched,
		store:     s,
		notifier:  notif,
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

	var finalStatus drevtypes.RunStatus

	if err := p.scheduler.RunPipeline(ctx, job.Pipeline, job.RunID, job.LogWriter); err != nil {
		log.Printf("[pool] run %s failed: %v", job.RunID, err)
		_ = p.store.UpdateRunStatus(ctx, job.RunID, drevtypes.StatusFailed)
		finalStatus = drevtypes.StatusFailed
	} else {
		log.Printf("[pool] run %s succeeded", job.RunID)
		// Safety net: ensure the run is marked as success even if the
		// scheduler's own status update was lost (e.g., due to a DB hiccup).
		_ = p.store.UpdateRunStatus(ctx, job.RunID, drevtypes.StatusSuccess)
		finalStatus = drevtypes.StatusSuccess
	}

	// Fire notification asynchronously — never block the worker
	if p.notifier != nil {
		run, err := p.store.GetRun(ctx, job.RunID)
		if err != nil {
			log.Printf("[pool] failed to get run for notification: %v", err)
			return
		}
		run.Status = finalStatus

		jobs, _ := p.store.GetRunJobs(ctx, job.RunID)

		go func() {
			if err := p.notifier.NotifyPipelineComplete(
				context.Background(),
				run,
				job.Pipeline,
				jobs,
			); err != nil {
				log.Printf("[pool] notification error for run %s: %v", job.RunID, err)
			}
		}()
	}
}
