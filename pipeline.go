package pipelines

import (
	"context"
)

// Pipeline handlers constraint.
type PipelineHandlers[T, U any] interface {
	Handler[T, U] | HandlerOption[T, U] |
		func(context.Context, EventWriter[U], Event[T])
}

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, N any, H PipelineHandlers[U, N]](c Pipeline[T, U], h H) Pipeline[T, N] {
	handler, workers := expandOptions[U, N](h)

	return Pipeline[T, N](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[N]) {
			w, r := c(ctx, workers)

			return w, startWorkers(ctx, handler, r, readers, workers)
		},
	)
}

// Creates new Pipeline[T, U].
func New[T, U any, H PipelineHandlers[T, U]](h H) Pipeline[T, U] {
	handler, workers := expandOptions[T, U](h)

	return Pipeline[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T](workers)
			r := startWorkers(ctx, handler, rw, readers, workers)

			return rw.GetWriter(), r
		},
	)
}

// Combination of Handlers into one Pipeline.
type Pipeline[T, U any] func(context.Context, int) (EventWriter[T], EventReader[U])

// Handles initial Event and returns result of Pipeline execution.
func (pipeline Pipeline[T, U]) Handle(ctx context.Context, e Event[T]) Result[U] {
	ctx, cancel := context.WithCancel(ctx)
	w, r := pipeline(ctx, 1)

	result := newResult(r.Read(), cancel)

	w.Write(e)
	w.Done()

	return result
}

func startWorkers[T, U any](
	ctx context.Context,
	handle Handler[T, U],
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
				handle(ctx, w, event)
			}

			w.Done()
		}()
	}

	return rw
}

func expandOptions[T, U any](v any) (Handler[T, U], int) {
	handle, ok := v.(func(context.Context, EventWriter[U], Event[T]))
	if ok {
		return wrapHandleWithErrHandle(handle, defaultErrorHandler[T, U]()), 0
	}

	opts, ok := v.(HandlerOption[T, U])
	if ok {
		return opts()
	}

	return wrapHandleWithErrHandle(v.(Handler[T, U]), defaultErrorHandler[T, U]()), 0
}
