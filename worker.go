package pipelines

import (
	"context"
	"errors"
	"sync"
)

var (
	WorkerStopped = errors.New("command worker is stopped")
)

// Asynchronous Pipeline
type Worker[T, U any] interface {
	// Asynchronously handles Event and returns error if Worker is stopped.
	Handle(T) error
	// returns false if Worker was stopped.
	IsRunning() bool
}

// Returns Worker based on Pipeline[T, U].
// eventSink is used to process the Result[U] of execution.
func NewWorker[T, U any](ctx context.Context, eventSink func(*Result[U]), pipeline Pipeline[T, U]) Worker[T, U] {
	w := &worker[T, U]{
		ctx:       ctx,
		started:   false,
		eventSink: eventSink,
		pipeline:  pipeline,
	}

	w.start()

	return w
}

type worker[T, U any] struct {
	ctx  context.Context
	rwMu sync.RWMutex

	pipeline  Pipeline[T, U]
	eventPipe chan T
	eventSink func(*Result[U])
	started   bool
}

func (w *worker[T, U]) Handle(payload T) error {
	w.rwMu.RLock()
	defer w.rwMu.RUnlock()

	if !w.started {
		return WorkerStopped
	}

	w.eventPipe <- payload

	return nil
}

func (w *worker[T, U]) IsRunning() bool {
	w.rwMu.RLock()
	defer w.rwMu.RUnlock()

	return w.started
}

func (w *worker[T, U]) start() {
	if w.started {
		return
	}

	w.rwMu.Lock()

	w.eventPipe = make(chan T)
	w.started = true

	w.rwMu.Unlock()

	go func() {
		var wg sync.WaitGroup
		shutdown := func() {
			w.rwMu.Lock()

			wg.Wait()

			w.started = false
			close(w.eventPipe)

			w.rwMu.Unlock()
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
