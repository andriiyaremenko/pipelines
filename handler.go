package pipelines

import (
	"context"
	"fmt"
)

// Handler is used to handle particular event.
type Handler[T, U any] func(context.Context, EventWriter[U], T)

func (h Handler[T, U]) Pipeline(opts ...HandlerOptions) Pipeline[T, U] {
	h = withRecovery(h)
	errHandler := defaultErrorHandler
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[U], int) {
		rw := newEventRW[T](ctx)

		return rw.GetWriter(), startWorkers(ctx, h, defaultErrorHandler, rw, 0), 0
	}
}

type ErrorHandler func(context.Context, ErrorWriter, error)

// HandlerOption returns Handler, error Handler and worker pool to use in Pipeline.
type HandlerOptions func(ErrorHandler, int) (ErrorHandler, int)

// Option to use with handler.
func WithOptions(errorHandler ErrorHandler, handlerPool int) HandlerOptions {
	return func(oldErrorHandler ErrorHandler, oldHandlerPool int) (ErrorHandler, int) {
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
func WithHandlerPool(size int) HandlerOptions {
	return WithOptions(nil, size)
}

// Option that specifies error handler to use along handler.
func WithErrorHandler(errorHandler ErrorHandler) HandlerOptions {
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

var defaultErrorHandler ErrorHandler = func(ctx context.Context, w ErrorWriter, err error) {
	w.WriteError(err)
}

func errHandleWithRecovery(handle ErrorHandler) ErrorHandler {
	return func(ctx context.Context, w ErrorWriter, err error) {
		defer func() {
			if r := recover(); r != nil {
				w.WriteError(NewError(fmt.Errorf("recovered from panic: %v", r), err))
			}
		}()

		handle(ctx, w, err)
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
