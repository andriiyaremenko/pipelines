package pipelines

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

var ErrWorkerStopped = errors.New("command worker is stopped")

// Asynchronous Pipeline
type Worker[T, U any] interface {
	// Asynchronously handles Event and returns error if Worker is stopped.
	Handle(T) error
	// returns false if Worker was stopped.
	IsRunning() bool
}

// Returns Worker based on `Pipeline[T, U]`.
// eventSink is used to process the `Result[U]` of execution.
func NewWorker[T, U any](ctx context.Context, eventSink func(iter.Seq2[U, error]), pipeline Pipeline[T, U]) Worker[T, U] {
	w := &worker[T, U]{
		ctx:       ctx,
		eventSink: eventSink,
		pipeline:  pipeline,
	}

	w.start()

	return w
}

type worker[T, U any] struct {
	ctx       context.Context
	pipeline  Pipeline[T, U]
	eventPipe chan T
	eventSink func(iter.Seq2[U, error])
	started   atomic.Bool
}

func (w *worker[T, U]) Handle(payload T) error {
	if !w.started.Load() {
		return ErrWorkerStopped
	}

	w.eventPipe <- payload

	return nil
}

func (w *worker[T, U]) IsRunning() bool {
	return w.started.Load()
}

func (w *worker[T, U]) start() {
	if w.started.Load() {
		return
	}

	w.eventPipe = make(chan T)
	w.started.Store(true)

	go func() {
		var wg sync.WaitGroup
		shutdown := func() {
			wg.Wait()

			w.started.Store(false)
			close(w.eventPipe)
		}

		for {
			select {
			case <-w.ctx.Done():
				shutdown()

				return
			default:
			}

			select {
			case <-w.ctx.Done():
				shutdown()

				return
			case event := <-w.eventPipe:
				wg.Add(1)
				go func() {
					defer wg.Done()

					ctx, cancel := context.WithCancel(w.ctx)

					w.eventSink(w.pipeline.Handle(ctx, event))

					cancel()
				}()
			}
		}
	}()
}
