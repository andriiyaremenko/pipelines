package pipelines

import (
	"context"

	"github.com/andriiyaremenko/pipelines/internal"
)

var _ Handler[any, any] = new(BaseHandler[any, any])

// *BaseHandler implements Handler.
type BaseHandler[T, U any] struct {
	HandleFunc func(ctx context.Context, w EventWriter[U], e Event[T])
	NWorkers   int
}

// Runs HandleFunc.
func (ch *BaseHandler[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	select {
	case <-ctx.Done():
		w.Write(NewErrHandlerEvent[U](internal.InstanceTypeName(ch), ctx.Err()))

		return
	default:
		ch.HandleFunc(ctx, w, event)
	}
}

func (ch *BaseHandler[T, U]) Workers() int {
	return ch.NWorkers
}

// Returns Handler with EventType equals eventType.
// and Handle based on handle.
func HandlerFunc[T, U any](handle func(context.Context, T) (U, error)) Handler[T, U] {
	return handlerFunc[T, U](handle)
}

type handlerFunc[T, U any] func(context.Context, T) (U, error)

func (handle handlerFunc[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	select {
	case <-ctx.Done():
		w.Write(NewErrHandlerEvent[U](internal.InstanceTypeName(handle), ctx.Err()))

		return
	default:
		v, err := handle(ctx, event.Payload())
		if err != nil {
			w.Write(NewErrHandlerEvent[U](internal.InstanceTypeName(handle), err))

			return
		}

		w.Write(E[U]{P: v})
		return
	}
}

func (handlerFunc[T, U]) Workers() int {
	return 0
}

type defaultErrorHandler[T, U any] struct{}

func (h *defaultErrorHandler[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	w.Write(NewErr[U](event.Err()))
}

func (h *defaultErrorHandler[T, U]) Workers() int {
	return 0
}

func withDefaultErrorHandler[T, U any](h Handler[T, U]) ErrorHandler[T, U] {
	return &errHandler[T, U]{handler: h, errorHandler: new(defaultErrorHandler[T, U])}
}

type errHandler[T, U any] struct {
	handler      Handler[T, U]
	errorHandler Handler[T, U]
}

func (h *errHandler[T, U]) Handle(ctx context.Context, w EventWriter[U], event Event[T]) {
	h.handler.Handle(ctx, w, event)
}

func (h *errHandler[T, U]) HandleError(ctx context.Context, w EventWriter[U], event Event[T]) {
	h.errorHandler.Handle(ctx, w, event)
}

func (h *errHandler[T, U]) Workers() int {
	return h.handler.Workers()
}
