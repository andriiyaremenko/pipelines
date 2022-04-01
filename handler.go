package pipelines

import (
	"context"
)

var (
	_ Handler[any, any] = Handle[any, any](nil)
	_ Handler[any, any] = HandleFunc[any, any](nil)
)

// Handles single type of Event.
type Handler[T, U any] interface {
	// Method to handle Events.
	Handle(ctx context.Context, w EventWriter[U], event Event[T])
}

// Handler Options constructor type.
type HandlerOption[T, U any] func() (Handler[T, U], Handler[T, U], int)

// Option to use with handler.
func WithOptions[T, U any](
	handler Handler[T, U],
	errorHandler Handler[T, U],
	workers int,
) HandlerOption[T, U] {
	return func() (Handler[T, U], Handler[T, U], int) {
		return handler, errorHandler, workers
	}
}

// Option that specifies worker pool size for handler.
func WithWorkerPool[T, U any](handler Handler[T, U], workers int) HandlerOption[T, U] {
	return WithOptions[T, U](handler, defaultErrorHandler[T, U](), workers)
}

// Option that specifies error handler to use along handler.
func WithErrorHandler[T, U any](
	handler Handler[T, U],
	errorHandler Handler[T, U],
) HandlerOption[T, U] {
	return WithOptions(handler, errorHandler, 0)
}

// Handle implements Handler[T, U].
type Handle[T, U any] func(context.Context, EventWriter[U], Event[T])

func (handle Handle[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	select {
	case <-ctx.Done():
		w.Write(NewErrHandlerEvent[U](handle, ctx.Err()))

		return
	default:
		handle(ctx, w, event)
	}
}

// HandleFunc implements Handler[T, U].
type HandleFunc[T, U any] func(context.Context, T) (U, error)

func (handle HandleFunc[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	select {
	case <-ctx.Done():
		w.Write(NewErrHandlerEvent[U](handle, ctx.Err()))

		return
	default:
		v, err := handle(ctx, event.Payload)
		if err != nil {
			w.Write(NewErrHandlerEvent[U](handle, err))

			return
		}

		w.Write(Event[U]{Payload: v})
		return
	}
}

func defaultErrorHandler[T, U any]() Handle[T, U] {
	return Handle[T, U](func(ctx context.Context, w EventWriter[U], event Event[T]) {
		w.Write(NewErr[U](event.Err))
	})
}
