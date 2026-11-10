package pool

import (
	"context"
	"fmt"
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
		if job.LogWriter != nil {
			job.LogWriter.Close()
		}
		log.Printf("[pool] run %s completed in %s", job.RunID, time.Since(start).Round(time.Millisecond))
	}()

	// Atomic durable execution claim: verify run is pending, set status to running,
	// record started_at, and delete its outbox entry before any pipeline side effects occur.
	if p.store != nil {
		if err := p.store.ClaimRunForExecution(ctx, job.RunID); err != nil {
			log.Printf("[pool] worker failed to claim run %s for execution: %v", job.RunID, err)
			if job.LogWriter != nil {
				fmt.Fprintf(job.LogWriter, "[drev] run %s claim rejected: %v\n", job.RunID, err)
			}
			return
		}
	}

	runErr := p.scheduler.RunPipeline(ctx, job.Pipeline, job.RunID, job.LogWriter)
	if runErr != nil {
		log.Printf("[pool] run %s finished with error: %v", job.RunID, runErr)
	} else {
		log.Printf("[pool] run %s succeeded", job.RunID)
	}

	if p.store == nil {
		return
	}

	// Retrieve the run once using an uncancelled context.
	// The scheduler is the sole authority for run status; the pool never mutates it.
	storedRun, err := p.store.GetRun(context.Background(), job.RunID)
	if err != nil {
		log.Printf("[pool] failed to get run %s from store: %v", job.RunID, err)
		return
	}
	if storedRun == nil {
		log.Printf("[pool] run %s not found in store", job.RunID)
		return
	}
	if !isTerminalRunStatus(storedRun.Status) {
		log.Printf("[pool] warning: run %s left in non-terminal status %q by scheduler, skipping notification", job.RunID, storedRun.Status)
		return
	}

	// Fire notification asynchronously with the exact stored run — never block the worker
	if p.notifier != nil {
		jobs, err := p.store.GetRunJobs(context.Background(), job.RunID)
		if err != nil {
			log.Printf("[pool] failed to get run jobs for %s: %v", job.RunID, err)
		}

		go func() {
			if err := p.notifier.NotifyPipelineComplete(
				context.Background(), storedRun, job.Pipeline, jobs,
			); err != nil {
				log.Printf("[pool] notification error for run %s: %v", job.RunID, err)
			}
		}()
	}
}

func isTerminalRunStatus(status drevtypes.RunStatus) bool {
	return status == drevtypes.StatusSuccess ||
		status == drevtypes.StatusFailed ||
		status == drevtypes.StatusCancelled
}
