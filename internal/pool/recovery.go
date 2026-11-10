package pool

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/store"
)

// LogStreamerProvider abstracts log stream writer creation for recovered runs.
type LogStreamerProvider interface {
	Writer(runID string) (io.WriteCloser, error)
}

// OutboxRecoverer recovers pending, never-started runs from durable outbox storage into the execution queue.
type OutboxRecoverer struct {
	store     store.Store
	queue     *queue.Queue
	streamer  LogStreamerProvider
	batchSize int
}

// NewOutboxRecoverer creates an OutboxRecoverer.
func NewOutboxRecoverer(s store.Store, q *queue.Queue, streamer LogStreamerProvider) *OutboxRecoverer {
	return &OutboxRecoverer{
		store:     s,
		queue:     q,
		streamer:  streamer,
		batchSize: 50,
	}
}

type recoveryNopWriteCloser struct{ io.Writer }

func (recoveryNopWriteCloser) Close() error { return nil }

// Recover reads pending outbox entries in bounded batches, parses their pipeline specifications,
// and enqueues them for execution. If the queue is full, it blocks until queue capacity is available
// or ctx is cancelled, guaranteeing that pending entries are never dropped due to queue capacity.
// Shutdown cleanly unblocks and exits without leaving orphaned goroutines.
func (r *OutboxRecoverer) Recover(ctx context.Context) error {
	if r.store == nil || r.queue == nil {
		return nil
	}

	seen := make(map[string]bool)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		items, err := r.store.GetPendingOutbox(ctx, r.batchSize)
		if err != nil {
			return fmt.Errorf("reading pending outbox: %w", err)
		}
		if len(items) == 0 {
			break
		}

		newItemsCount := 0
		for _, item := range items {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if seen[item.RunID] {
				continue
			}
			seen[item.RunID] = true
			newItemsCount++

			// Parse pipeline spec
			pipeline, err := parser.ParseBytes([]byte(item.PipelineSpec))
			if err != nil {
				log.Printf("[recovery] invalid pipeline spec for run %s: %v", item.RunID, err)
				// Atomically fail run and clear corrupted outbox entry so it doesn't block recovery
				_ = r.store.FailRunWithDelivery(ctx, item.RunID, nil, "")
				continue
			}

			// Obtain log writer
			var logW io.WriteCloser
			if r.streamer != nil {
				lw, err := r.streamer.Writer(item.RunID)
				if err != nil {
					log.Printf("[recovery] failed to create log writer for run %s: %v", item.RunID, err)
					_ = r.store.FailRunWithDelivery(ctx, item.RunID, nil, "")
					continue
				}
				logW = lw
			} else {
				logW = recoveryNopWriteCloser{io.Discard}
			}

			job := &queue.Job{
				RunID:      item.RunID,
				Pipeline:   pipeline,
				LogWriter:  logW,
				EnqueuedAt: time.Now(),
			}

			// Capacity-aware enqueue: blocks until queue space is available or ctx cancelled
			if err := r.queue.EnqueueWithContext(ctx, job); err != nil {
				if logW != nil {
					logW.Close()
				}
				return err
			}
		}

		// If no new items were found in this batch (all returned items are already enqueued in memory),
		// all pending outbox entries have been safely submitted to the queue.
		if newItemsCount == 0 {
			break
		}
	}

	return nil
}
