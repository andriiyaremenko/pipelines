package pipelines

import (
	"context"
	"sync"
	"sync/atomic"
)

// Serves to pass Events to Handlers.
type EventReader[T any] interface {
	// Returns Event[T] channel.
	Read() <-chan *Event[T]

	// Disposes the event
	Dispose(*Event[T])

	// Returns EventWriter instance on which this EventReader is based.
	GetWriter() EventWriterCloser[T]
}

// Serves to write Events in Handle.Handle to chain Events.
type ErrorWriter interface {
	// Writes Event to a channel.
	WriteError(err error)
}

// Serves to write Events in Handle.Handle to chain Events.
type EventWriter[T any] interface {
	// Writes Event to a channel.
	Write(e T)
	ErrorWriter
}

// Serves to close EventWriter.
type EventCloser interface {
	// Signals that no more writes are expected.
	Close()
}

type EventWriterCloser[T any] interface {
	EventWriter[T]
	EventCloser
}

func newEventRW[T any](ctx context.Context) EventReader[T] {
	rw := &eventRW[T]{
		ctx:           ctx,
		eventsChannel: make(chan *Event[T]),
		pool: &sync.Pool{
			New: func() any {
				return &Event[T]{}
			},
		},
	}

	return rw
}

type eventRW[T any] struct {
	pool          *sync.Pool
	ctx           context.Context
	eventsChannel chan *Event[T]
	writersGroup  sync.WaitGroup
	shutdown      sync.Once
}

func (r *eventRW[T]) Read() <-chan *Event[T] {
	return r.eventsChannel
}

func (r *eventRW[T]) Dispose(e *Event[T]) {
	r.pool.Put(e)
}

func (r *eventRW[T]) GetWriter() EventWriterCloser[T] {
	r.writersGroup.Add(1)

	r.shutdown.Do(func() {
		go func() {
			r.writersGroup.Wait()
			close(r.eventsChannel)
		}()
	})

	events := make(chan *Event[T])
	w := &eventW[T]{events: events, ctx: r.ctx, pool: r.pool}

	go func() {
		for e := range events {
			r.eventsChannel <- e
		}

		r.writersGroup.Done()
	}()

	return w
}

type eventW[T any] struct {
	pool    *sync.Pool
	ctx     context.Context
	events  chan *Event[T]
	writeWG sync.WaitGroup
	once    sync.Once
	isDone  atomic.Bool
}

func (w *eventW[T]) Write(e T) {
	if !w.isDone.Load() {
		w.writeWG.Add(1)
		go func() {
			select {
			case <-w.ctx.Done():
			default:
				if !w.isDone.Load() {
					event := w.pool.Get().(*Event[T])
					event.Payload = e
					w.events <- event
				}
			}

			w.writeWG.Done()
		}()
	}
}

func (w *eventW[T]) WriteError(err error) {
	if !w.isDone.Load() {
		w.writeWG.Add(1)
		go func() {
			select {
			case <-w.ctx.Done():
			default:
				if !w.isDone.Load() {
					event := w.pool.Get().(*Event[T])
					event.Err = err
					w.events <- event
				}
			}

			w.writeWG.Done()
		}()
	}
}

func (w *eventW[T]) Close() {
	go w.once.Do(func() {
		w.writeWG.Wait()
		w.isDone.Store(true)
		close(w.events)
	})
}
