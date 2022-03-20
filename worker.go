package pipelines

import (
	"context"
	"errors"
	"sync"
)

var (
	WorkerStopped = errors.New("command worker is stopped")
)

// Returns CommandWorker based on Commands.
// eventSink is used to channel all unhandled errors in form of Event.
func NewWorker[T, U any](ctx context.Context, eventSink func(Result[U]), chain Pipeline[T, U]) Worker[T, U] {
	w := &worker[T, U]{
		ctx:       ctx,
		started:   false,
		eventSink: eventSink,
		chain:     chain,
	}

	w.start()

	return w
}

type worker[T, U any] struct {
	ctx  context.Context
	rwMu sync.RWMutex

	started   bool
	chain     Pipeline[T, U]
	eventPipe chan Event[T]
	eventSink func(Result[U])
}

func (w *worker[T, U]) Handle(event Event[T]) error {
	w.rwMu.RLock()
	defer w.rwMu.RUnlock()

	if !w.started {
		return WorkerStopped
	}

	w.eventPipe <- event

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

	w.eventPipe = make(chan Event[T])
	w.started = true

	w.rwMu.Unlock()

	go func() {
		var wg sync.WaitGroup

		for {
			select {
			case <-w.ctx.Done():
				w.rwMu.Lock()

				wg.Wait()

				w.started = false
				close(w.eventPipe)

				w.rwMu.Unlock()

				return
			case event := <-w.eventPipe:
				wg.Add(1)
				go func() {
					defer wg.Done()

					ctx, cancel := context.WithCancel(w.ctx)

					defer cancel()
					w.eventSink(w.chain.Handle(ctx, event))
				}()
			}
		}
	}()
}
