package pipelines

import (
	"context"
	"iter"
)

// Creates new `Pipeline[T, U]`.
func New[T, U any](h Handler[T, U], opts ...HandlerOptions[U]) Pipeline[T, U] {
	h = withRecovery(h)
	errHandler := defaultErrorHandler[U]()
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[U], int) {
		rw := newEventRW[T](ctx)

		return rw.GetWriter(), startWorkers(ctx, h, errHandler, rw, pool), pool
	}
}

// Adds next `Handler[U, H]` to the `Pipeline[T, U]` resulting in new `Pipeline[T, H]`.
func Pipe[T, U, N any, P Pipeline[T, U]](c P, h Handler[U, N], opts ...HandlerOptions[N]) Pipeline[T, N] {
	h = withRecovery(h)
	errHandler := defaultErrorHandler[N]()
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[N], int) {
		w, r, oldPool := c(ctx)
		if pool < oldPool {
			pool = oldPool
		}

		return w, startWorkers(ctx, h, errHandler, r, pool), pool
	}
}

// Adds error Handler to the `Pipeline[T, U]` resulting in new `Pipeline[T, U]`.
func PipeErrorHandler[T, U any](c Pipeline[T, U], h Handler[error, U]) Pipeline[T, U] {
	h = withRecovery(h)

	return func(ctx context.Context) (EventWriterCloser[T], EventReader[U], int) {
		w, r, pool := c(ctx)

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
	errHandle Handler[error, U],
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
			}

			w.Close()
		}()
	}

	return rw
}
