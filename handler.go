package pipelines

import (
	"context"
	"fmt"
)

// Handler is used to handle particular event.
type Handler[T, U any] func(context.Context, EventWriter[U], T)

// HandlerOption returns Handler, error Handler and worker pool to use in Pipeline.
type HandlerOptions[T any] func(Handler[error, T], int) (Handler[error, T], int)

// Option to use with handler.
func WithOptions[T any](errorHandler Handler[error, T], handlerPool int) HandlerOptions[T] {
	return func(oldErrorHandler Handler[error, T], oldHandlerPool int) (Handler[error, T], int) {
		if errorHandler == nil {
			errorHandler = oldErrorHandler
		}

		if handlerPool < 1 {
			handlerPool = oldHandlerPool
		}

		return errorHandler, handlerPool
	}
}

// Option that specifies handler pool size.
func WithHandlerPool[T any](size int) HandlerOptions[T] {
	return WithOptions[T](nil, size)
}

// Option that specifies error handler to use along handler.
func WithErrorHandler[T any](errorHandler Handler[error, T]) HandlerOptions[T] {
	return WithOptions(errorHandler, 0)
}

// Handler that writes same payload it receives without changes.
func PassThrough[T any]() Handler[T, T] {
	return func(ctx context.Context, w EventWriter[T], payload T) {
		w.Write(payload)
	}
}

// HandleFunc returns Handler function.
func HandleFunc[T, U any](handle Handle[T, U]) Handler[T, U] {
	return func(ctx context.Context, w EventWriter[U], payload T) {
		v, err := handle(ctx, payload)
		if err != nil {
			w.WriteError(NewError(err, payload))

			return
		}

		w.Write(v)
	}
}

func defaultErrorHandler[U any]() Handler[error, U] {
	return func(ctx context.Context, w EventWriter[U], err error) {
		w.WriteError(err)
	}
}

func withRecovery[T, U any, H Handler[T, U]](handle H) H {
	return func(ctx context.Context, w EventWriter[U], payload T) {
		defer func() {
			if r := recover(); r != nil {
				w.WriteError(NewError(fmt.Errorf("recovered from panic: %v", r), payload))
			}
		}()

		handle(ctx, w, payload)
	}
}
