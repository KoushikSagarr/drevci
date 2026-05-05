package queue

import (
	"io"
	"testing"
	"time"

	"github.com/drevci/drev/pkg/drevtypes"
)

// nopWriteCloser wraps an io.Writer to implement io.WriteCloser.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func makeJob(id string) *Job {
	return &Job{
		RunID:      id,
		Pipeline:   &drevtypes.Pipeline{Name: id},
		LogWriter:  nopWriteCloser{io.Discard},
		EnqueuedAt: time.Now(),
	}
}

func TestEnqueue_And_Drain(t *testing.T) {
	q := New(10)

	ids := []string{"a", "b", "c"}
	for _, id := range ids {
		if err := q.Enqueue(makeJob(id)); err != nil {
			t.Fatalf("unexpected enqueue error: %v", err)
		}
	}

	ch := q.Drain()
	for _, want := range ids {
		job := <-ch
		if job.RunID != want {
			t.Errorf("expected RunID %q, got %q", want, job.RunID)
		}
	}
}

func TestEnqueue_Full(t *testing.T) {
	q := New(1)

	if err := q.Enqueue(makeJob("first")); err != nil {
		t.Fatalf("first enqueue should succeed: %v", err)
	}

	if err := q.Enqueue(makeJob("second")); err == nil {
		t.Fatal("second enqueue should fail when queue is full")
	}
}

func TestDepth(t *testing.T) {
	q := New(10)
	q.Enqueue(makeJob("x"))
	q.Enqueue(makeJob("y"))

	if d := q.Depth(); d != 2 {
		t.Errorf("expected depth 2, got %d", d)
	}
}
