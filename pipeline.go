package pipelines

import (
	"context"
)

// Combination of Handlers into one Pipeline.
type Pipeline[T, U any] interface {
	// Handles initial Event and returns result of Pipeline execution.
	Handle(context.Context, Event[T]) Result[U]
	// Starts workers to handle incoming Event.
	Spin(context.Context, int) (EventWriter[T], EventReader[U])
}

// Pipeline handlers constraint.
type PipelineHandlers[T, U any] interface {
	Handler[T, U] | HandlerOption[T, U] |
		func(context.Context, EventWriter[U], Event[T])
}

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, N any, H PipelineHandlers[U, N]](c Pipeline[T, U], h H) Pipeline[T, N] {
	handler, errHandler, workers := expandOptions[U, N](h)
	handler = wrapHandle(handler)

	return flow[T, N](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[N]) {
			w, r := c.Spin(ctx, workers)

			return w, startWorkers(ctx, handler, errHandler, r, readers, workers)
		},
	)
}

// Creates new Pipeline[T, U].
func New[T, U any, H PipelineHandlers[T, U]](h H) Pipeline[T, U] {
	handler, errHandler, workers := expandOptions[T, U](h)
	handler = wrapHandle(handler)

	return flow[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T](workers)
			r := startWorkers(ctx, handler, errHandler, rw, readers, workers)

			return rw.GetWriter(), r
		},
	)
}

type flow[T, U any] func(context.Context, int) (EventWriter[T], EventReader[U])

func (spin flow[T, U]) Handle(ctx context.Context, e Event[T]) Result[U] {
	ctx, cancel := context.WithCancel(ctx)
	w, r := spin(ctx, 1)

	result := newResult(r.Read(), cancel)

	w.Write(e)
	w.Done()

	return result
}

func (spin flow[T, U]) Spin(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
	return spin(ctx, readers)
}

func startWorkers[T, U any](
	ctx context.Context,
	handle, handleError Handler[T, U],
	r EventReader[T],
	readers, workers int,
) EventReader[U] {
	rw := newEventRW[U](readers)

	if workers == 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		w := rw.GetWriter()
		go func() {
			for event := range r.Read() {
				if event.Err != nil {
					handleError(ctx, w, event)
					continue
				}

				handle(ctx, w, event)
			}
			w.Done()
		}()
	}

	return rw
}

func expandOptions[T, U any](v any) (Handler[T, U], Handler[T, U], int) {
	opts, ok := v.(HandlerOption[T, U])
	if ok {
		return opts()
	}

	handle, ok := v.(func(context.Context, EventWriter[U], Event[T]))
	if ok {
		return handle, defaultErrorHandler[T, U](), 0
	}

	return v.(Handler[T, U]), defaultErrorHandler[T, U](), 0
}
