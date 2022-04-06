package pipelines

import (
	"context"
)

// Pipeline handler constraint.
type PipelineHandlers[T, U any] interface {
	Handler[T, U] | HandlerOption[T, U]
	ExpandOptions() (Handler[T, U], Handler[error, U], int)
}

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, N any, H PipelineHandlers[U, N]](c Pipeline[T, U], h H) Pipeline[T, N] {
	handler, errHandle, workers := h.ExpandOptions()

	return Pipeline[T, N](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[N]) {
			w, r := c(ctx, workers)

			return w, startWorkers(ctx, handler, errHandle, r, readers, workers)
		},
	)
}

// Creates new Pipeline[T, U].
func New[T, U any, H PipelineHandlers[T, U]](h H) Pipeline[T, U] {
	handler, errHandler, workers := h.ExpandOptions()

	return Pipeline[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T](workers)
			r := startWorkers(ctx, handler, errHandler, rw, readers, workers)

			return rw.GetWriter(), r
		},
	)
}

// Combination of Handlers into one Pipeline.
type Pipeline[T, U any] func(context.Context, int) (EventWriter[T], EventReader[U])

// Handles initial Event and returns result of Pipeline execution.
func (pipeline Pipeline[T, U]) Handle(ctx context.Context, payload T) *Result[U] {
	ctx, cancel := context.WithCancel(ctx)
	w, r := pipeline(ctx, 1)

	result := newResult(r.Read(), cancel)

	w.Write(Event[T]{Payload: payload})
	w.Done()

	return result
}

func startWorkers[T, U any](
	ctx context.Context,
	handle Handler[T, U],
	errHandle Handler[error, U],
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
					errHandle(ctx, w, event.Err)

					continue
				}

				handle(ctx, w, event.Payload)
			}

			w.Done()
		}()
	}

	return rw
}
