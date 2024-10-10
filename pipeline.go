package pipelines

import (
	"context"
	"iter"
)

// Adds next `Handler[U, H]` to the `Pipeline[T, U]` resulting in new `Pipeline[T, H]`.
func Pipe[T, U, N any, P Pipeline[T, U]](p P, h Handler[U, N], opts ...HandlerOptions) Pipeline[T, N] {
	h = withRecovery(h)
	errHandler := defaultErrorHandler
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[N], int) {
		w, r, oldPool := p(ctx)
		if pool < oldPool {
			pool = oldPool
		}

		return w, startWorkers(ctx, h, errHandler, r, pool), pool
	}
}

func Pipe2[T, U, N, S any, P Pipeline[T, U]](p P, h1 Handler[U, N], h2 Handler[N, S], opts ...HandlerOptions) Pipeline[T, S] {
	errHandler := defaultErrorHandler
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	_p := Pipe(p, h1, WithHandlerPool(pool))
	h2 = withRecovery(h2)

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[S], int) {
		w, r, oldPool := _p(ctx)
		if pool < oldPool {
			pool = oldPool
		}

		return w, startWorkers(ctx, h2, errHandler, r, pool), pool
	}
}

func Pipe3[T, U, N, S, Y any, P Pipeline[T, U]](
	p P, h1 Handler[U, N], h2 Handler[N, S], h3 Handler[S, Y], opts ...HandlerOptions,
) Pipeline[T, Y] {
	errHandler := defaultErrorHandler
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	_p := Pipe2(p, h1, h2, WithHandlerPool(pool))
	h3 = withRecovery(h3)

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[Y], int) {
		w, r, oldPool := _p(ctx)
		if pool < oldPool {
			pool = oldPool
		}

		return w, startWorkers(ctx, h3, errHandler, r, pool), pool
	}
}

func Pipe4[T, U, N, S, Y, X any, P Pipeline[T, U]](
	p P, h1 Handler[U, N], h2 Handler[N, S], h3 Handler[S, Y], h4 Handler[Y, X], opts ...HandlerOptions,
) Pipeline[T, X] {
	errHandler := defaultErrorHandler
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	_p := Pipe3(p, h1, h2, h3, WithHandlerPool(pool))
	h4 = withRecovery(h4)

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[X], int) {
		w, r, oldPool := _p(ctx)
		if pool < oldPool {
			pool = oldPool
		}

		return w, startWorkers(ctx, h4, errHandler, r, pool), pool
	}
}

// Adds error Handler to the `Pipeline[T, U]` resulting in new `Pipeline[T, U]`.
func PipeErrorHandler[T, U any](p Pipeline[T, U], h ErrorHandler) Pipeline[T, U] {
	h = errHandleWithRecovery(h)

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[U], int) {
		w, r, pool := p(ctx)

		return w, startWorkers(ctx, PassThrough[U](), h, r, pool), pool
	}
}

// Combination of Handlers into one Pipeline.
type Pipeline[T, U any] func(context.Context) (EventWriterCloser[T], EventReader[U], int)

// Handles initial Event and returns result of Pipeline execution.
func (pipeline Pipeline[T, U]) Handle(ctx context.Context, payload T) iter.Seq2[U, error] {
	return func(yield func(U, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		w, r, _ := pipeline(ctx)

		defer func() {
			cancel()

			// To avoid stacked goroutines we need to exhaust EventReader.Read() channel.
			// Cancelling ctx will stop next writes, but due to it being executed in different goroutines
			// might result into couple dropped events
			for range r.Read() {
			}
		}()

		w.Write(payload)
		w.Close()

		for e := range r.Read() {
			if !yield(e.Payload, e.Err) {
				return
			}
		}
	}
}

func startWorkers[T, U any](
	ctx context.Context,
	handle Handler[T, U],
	errHandle ErrorHandler,
	r EventReader[T],
	workers int,
) EventReader[U] {
	rw := newEventRW[U](ctx)

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
				r.Dispose(event)
			}

			w.Close()
		}()
	}

	return rw
}
