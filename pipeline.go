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
	Handle[T, U] | HandleFunc[T, U] | HandlerOption[T, U] |
		func(context.Context, EventWriter[U], Event[T]) | func(context.Context, T) (U, error)
}

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, N any, H PipelineHandlers[U, N]](c Pipeline[T, U], h H) Pipeline[T, N] {
	handler, errHandler, workers := expandOptions[U, N](h)

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
	handler, errorHandler Handler[T, U],
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
					errorHandler.Handle(ctx, w, event)
					continue
				}

				handler.Handle(ctx, w, event)
			}
			w.Done()
		}()
	}

	return rw
}

func expandOptions[T, U any](v any) (Handler[T, U], Handler[T, U], int) {
	handle, ok := v.(func(context.Context, EventWriter[U], Event[T]))
	if ok {
		return Handle[T, U](handle), defaultErrorHandler[T, U](), 0
	}

	handleFunc, ok := v.(func(context.Context, T) (U, error))
	if ok {
		return HandleFunc[T, U](handleFunc), defaultErrorHandler[T, U](), 0
	}

	opts, ok := v.(HandlerOption[T, U])
	if ok {
		return opts()
	}

	return v.(Handler[T, U]), defaultErrorHandler[T, U](), 0
}
