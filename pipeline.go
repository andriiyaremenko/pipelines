package pipelines

import (
	"context"
)

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, H any](c Pipeline[T, U], h Handler[U, H]) Pipeline[T, H] {
	return flow[T, H](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[H]) {
			w, r := c.Spin(ctx, h.Workers())
			handler, hasErrorHandler := h.(ErrorHandler[U, H])

			if !hasErrorHandler {
				handler = withDefaultErrorHandler(h)
			}

			return w, startWorkers(ctx, handler, r, readers)
		},
	)
}

// Creates new Pipeline[T, U].
func New[T, U any](h Handler[T, U]) Pipeline[T, U] {
	return flow[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T](h.Workers())
			handler, hasErrorHandler := h.(ErrorHandler[T, U])

			if !hasErrorHandler {
				handler = withDefaultErrorHandler(h)
			}

			r := startWorkers(ctx, handler, rw, readers)

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

func startWorkers[T, U any](ctx context.Context, handler ErrorHandler[T, U], r EventReader[T], readers int) EventReader[U] {
	rw := newEventRW[U](readers)
	workers := handler.Workers()

	if workers == 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		w := rw.GetWriter()
		go func() {
			for event := range r.Read() {
				if event.Err != nil {
					handler.HandleError(ctx, w, event)
					continue
				}

				handler.Handle(ctx, w, event)
			}
			w.Done()
		}()
	}

	return rw
}
