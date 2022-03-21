package pipelines

import (
	"context"
	"fmt"

	"github.com/andriiyaremenko/pipelines/internal"
)

// Adds next Handler to the Pipeline.
func Append[T, U, H any](c Pipeline[T, U], h Handler[U, H]) Pipeline[T, H] {
	return flow[T, H](
		func(ctx context.Context) (EventWriter[T], EventReader[H]) {
			w, r := c.Spin(ctx)
			handler, hasErrorHandler := h.(ErrorHandler[U, H])

			if !hasErrorHandler {
				handler = withDefaultErrorHandler(h)
			}

			return w, startWorkers(ctx, handler, r)
		},
	)
}

// Creates new Pipeline.
func New[T, U any](h Handler[T, U]) Pipeline[T, U] {
	return flow[T, U](
		func(ctx context.Context) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T]()
			handler, hasErrorHandler := h.(ErrorHandler[T, U])

			if !hasErrorHandler {
				handler = withDefaultErrorHandler(h)
			}

			r := startWorkers(ctx, handler, rw)

			return rw.GetWriter(), r
		},
	)
}

// Combines two handlers to handle events and errors of the same event type.
func WithErrorHandler[T, U any](handler Handler[T, U], errorHandler Handler[T, U]) ErrorHandler[T, U] {
	return &errHandler[T, U]{handler: handler, errorHandler: errorHandler}
}

type flow[T, U any] func(context.Context) (EventWriter[T], EventReader[U])

func (spin flow[T, U]) Handle(ctx context.Context, e Event[T]) Result[U] {
	result := newResult[U](internal.TypeName[Event[T]]())

	if e == nil {
		result.AppendError(ErrNilEvent[T](fmt.Sprintf("%s.Handle failed", internal.TypeName[Pipeline[T, U]]())))

		return result
	}

	w, r := spin(ctx)

	w.Write(e)
	w.Done()

	for ev := range r.Read() {
		if err := ev.Err(); err != nil {
			result.AppendError(err)
			continue
		}

		result.Append(ev)
	}

	return result
}

func (spin flow[T, U]) Spin(ctx context.Context) (EventWriter[T], EventReader[U]) {
	return spin(ctx)
}

func startWorkers[T, U any](ctx context.Context, handler ErrorHandler[T, U], r EventReader[T]) EventReader[U] {
	rw := newEventRW[U]()
	workers := handler.Workers()

	if workers == 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		w := rw.GetWriter()
		go func() {
			for event := range r.Read() {
				if event == nil {
					err := ErrNilEvent[T](fmt.Sprintf("%s.Handle failed", internal.TypeName[Pipeline[T, U]]()))
					handler.HandleError(ctx, w, NewErrHandlerEvent[T](handler, err))

					continue
				}

				err := event.Err()
				if err != nil {
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
