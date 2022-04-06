package pipelines

import (
	"context"
)

// Handler is used to handle particular event.
type Handler[T, U any] func(context.Context, EventWriter[U], T)

func (h Handler[T, U]) ExpandOptions() (Handler[T, U], Handler[error, U], int) {
	return h, defaultErrorHandler[U](), 0
}

// HandlerOption returns Handler, error Handler and worker pool to use in Pipeline.
type HandlerOption[T, U any] func() (Handler[T, U], Handler[error, U], int)

func (ho HandlerOption[T, U]) ExpandOptions() (Handler[T, U], Handler[error, U], int) {
	return ho()
}

// Option to use with handler.
func WithOptions[T, U any](handler Handler[T, U], errorHandler Handler[error, U], workers int) HandlerOption[T, U] {
	return func() (Handler[T, U], Handler[error, U], int) {
		return handler, errorHandler, workers
	}
}

// Option that specifies worker pool size for handler.
func WithWorkerPool[T, U any](handler Handler[T, U], workers int) HandlerOption[T, U] {
	return WithOptions(handler, defaultErrorHandler[U](), workers)
}

// Option that specifies error handler to use along handler.
func WithErrorHandler[T, U any](handler Handler[T, U], errorHandler Handler[error, U]) HandlerOption[T, U] {
	return WithOptions(handler, errorHandler, 0)
}

// HandleFunc returns Handler function.
func HandleFunc[T, U any](handle Handle[T, U]) Handler[T, U] {
	return func(ctx context.Context, w EventWriter[U], payload T) {
		v, err := handle(ctx, payload)
		if err != nil {
			w.Write(NewErrEvent[U](NewError(err, payload)))

			return
		}

		w.Write(Event[U]{Payload: v})
	}
}

func defaultErrorHandler[U any]() Handler[error, U] {
	return func(ctx context.Context, w EventWriter[U], err error) {
		w.Write(NewErrEvent[U](err))
	}
}
