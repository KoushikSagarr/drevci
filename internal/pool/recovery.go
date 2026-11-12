package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/drevci/drev/internal/parser"
	"github.com/drevci/drev/internal/queue"
	"github.com/drevci/drev/internal/store"
	"github.com/drevci/drev/pkg/drevtypes"
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
			pipeline, err := deserializeRecoveredPipeline([]byte(item.PipelineSpec))
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

// deserializeRecoveredPipeline deserializes a persisted pipeline specification from the outbox.
// It strictly expects standard JSON payloads matching the outbox persistence format.
// If the pipeline defines jobs (populated spec), it validates structural and semantic correctness
// using parser.Validate. If the pipeline has zero jobs (bootstrap spec), it validates bootstrap
// source metadata so the worker can discover jobs after checkout.
func deserializeRecoveredPipeline(data []byte) (*drevtypes.Pipeline, error) {
	var p drevtypes.Pipeline

	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parsing pipeline spec JSON: %w", err)
	}

	if strings.TrimSpace(p.Name) == "" {
		return nil, errors.New("pipeline spec must have a name")
	}

	if len(p.Jobs) > 0 {
		// Populated pipeline specification: enforce strict structural and DAG validation
		if err := parser.Validate(&p); err != nil {
			return nil, fmt.Errorf("validating populated pipeline spec: %w", err)
		}
	} else {
		// Bootstrap pipeline specification: validate source checkout metadata
		if err := validateBootstrapSource(p.Source); err != nil {
			return nil, fmt.Errorf("validating bootstrap source: %w", err)
		}
	}

	return &p, nil
}

// validateBootstrapSource verifies that a bootstrap specification contains valid repository metadata.
func validateBootstrapSource(src drevtypes.Source) error {
	rawURL := strings.TrimSpace(src.URL)
	if rawURL == "" {
		return errors.New("bootstrap pipeline source URL cannot be empty")
	}
	if strings.HasPrefix(rawURL, "-") {
		return fmt.Errorf("invalid bootstrap source URL %q: cannot start with '-'", rawURL)
	}

	rawRef := strings.TrimSpace(src.Ref)
	if strings.HasPrefix(rawRef, "-") {
		return fmt.Errorf("invalid bootstrap source ref %q: cannot start with '-'", rawRef)
	}

	rawCommitSHA := strings.TrimSpace(src.CommitSHA)
	if strings.HasPrefix(rawCommitSHA, "-") {
		return fmt.Errorf("invalid bootstrap commit SHA %q: cannot start with '-'", rawCommitSHA)
	}
	if rawCommitSHA != "" && !drevtypes.IsCommitSHA(rawCommitSHA) {
		return fmt.Errorf("invalid bootstrap commit SHA %q: must be a 40-character (SHA-1) or 64-character (SHA-256) hexadecimal hash", rawCommitSHA)
	}

	if src.PRNumber < 0 {
		return fmt.Errorf("invalid bootstrap PR number %d: cannot be negative", src.PRNumber)
	}

	// A bootstrap specification must provide at least one usable checkout target
	if rawRef == "" && rawCommitSHA == "" {
		return errors.New("bootstrap pipeline source must specify at least one checkout target (Ref or CommitSHA)")
	}

	// PR pipelines must have a mandatory valid CommitSHA
	if src.PRNumber > 0 {
		if rawCommitSHA == "" {
			return fmt.Errorf("bootstrap PR pipeline (PR #%d) must specify a CommitSHA", src.PRNumber)
		}
		if !drevtypes.IsCommitSHA(rawCommitSHA) {
			return fmt.Errorf("invalid bootstrap commit SHA %q for PR #%d: must be a 40-character (SHA-1) or 64-character (SHA-256) hexadecimal hash", rawCommitSHA, src.PRNumber)
		}
	}

	return nil
}
