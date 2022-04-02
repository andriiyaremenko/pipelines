package pipelines

import (
	"context"
)

// Handle implements Handler[T, U].
type Handler[T, U any] func(context.Context, EventWriter[U], Event[T])

// Handler Options constructor type.
type HandlerOption[T, U any] func() (Handler[T, U], int)

// Option to use with handler.
func WithOptions[T, U any](handler, errorHandler Handler[T, U], workers int) HandlerOption[T, U] {
	return func() (Handler[T, U], int) {
		return wrapHandleWithErrHandle(handler, errorHandler), workers
	}
}

// Option that specifies worker pool size for handler.
func WithWorkerPool[T, U any](handler Handler[T, U], workers int) HandlerOption[T, U] {
	return WithOptions(handler, defaultErrorHandler[T, U](), workers)
}

// Option that specifies error handler to use along handler.
func WithErrorHandler[T, U any](handler, errorHandler Handler[T, U]) HandlerOption[T, U] {
	return WithOptions(handler, errorHandler, 0)
}

// HandleFunc returns handler function.
func HandleFunc[T, U any](handle func(context.Context, T) (U, error)) Handler[T, U] {
	return func(ctx context.Context, w EventWriter[U], event Event[T]) {
		v, err := handle(ctx, event.Payload)
		if err != nil {
			w.Write(NewErrHandlerEvent[U](handle, err))

			return
		}

		w.Write(Event[U]{Payload: v})
	}
}

func defaultErrorHandler[T, U any]() Handler[T, U] {
	return (func(ctx context.Context, w EventWriter[U], event Event[T]) {
		w.Write(NewErr[U](event.Err))
	})
}

func wrapHandleWithErrHandle[T, U any](handle, handleError Handler[T, U]) Handler[T, U] {
	return func(ctx context.Context, w EventWriter[U], event Event[T]) {
		if event.Err != nil {
			handleError(ctx, w, event)
			return
		}

		select {
		case <-ctx.Done():
			w.Write(NewErrHandlerEvent[U](handle, ctx.Err()))

			return
		default:
			handle(ctx, w, event)
		}
	}
}
