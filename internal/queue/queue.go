package queue

import (
	"fmt"
	"io"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// Job represents a pipeline execution request in the queue.
type Job struct {
	RunID      string
	Pipeline   *drevtypes.Pipeline
	LogWriter  io.WriteCloser
	EnqueuedAt time.Time
}

// Queue is a thread-safe in-memory buffered job queue.
type Queue struct {
	ch   chan *Job
	size int
}

// New creates a new Queue with the given buffer size.
func New(size int) *Queue {
	return &Queue{
		ch:   make(chan *Job, size),
		size: size,
	}
}

// Enqueue adds a job to the queue. Returns an error if the queue is full.
func (q *Queue) Enqueue(job *Job) error {
	select {
	case q.ch <- job:
		return nil
	default:
		return fmt.Errorf("queue is full (%d jobs pending)", q.size)
	}
}

// Drain returns the underlying channel for workers to consume from.
func (q *Queue) Drain() <-chan *Job {
	return q.ch
}

// Depth returns the number of jobs currently waiting in the queue.
func (q *Queue) Depth() int {
	return len(q.ch)
}
