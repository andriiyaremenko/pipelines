package pipelines

import (
	"context"
)

// Creates new Pipeline[T, U].
func New[T, U any](h Handler[T, U], opts ...HandlerOptions[U]) Pipeline[T, U] {
	errHandler := defaultErrorHandler[U]()
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return Pipeline[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			rw := newEventRW[T](pool)
			r := startWorkers(ctx, h, errHandler, rw, readers, pool)

			return rw.GetWriter(), r
		},
	)
}

// Adds next Handler[U, H] to the Pipeline[T, U] resulting in new Pipeline[T, H].
func Append[T, U, N any](c Pipeline[T, U], h Handler[U, N], opts ...HandlerOptions[N]) Pipeline[T, N] {
	errHandler := defaultErrorHandler[N]()
	pool := 0

	for _, option := range opts {
		errHandler, pool = option(errHandler, pool)
	}

	return Pipeline[T, N](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[N]) {
			w, r := c(ctx, pool)

			return w, startWorkers(ctx, h, errHandler, r, readers, pool)
		},
	)
}

// Adds error Handler to the Pipeline[T, U] resulting in new Pipeline[T, U].
func AppendErrorHandler[T, U any](c Pipeline[T, U], h Handler[error, U]) Pipeline[T, U] {
	return Pipeline[T, U](
		func(ctx context.Context, readers int) (EventWriter[T], EventReader[U]) {
			w, r := c(ctx, readers)

			return w, startWorkers(ctx, PassThrough[U](), h, r, readers, readers)
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
