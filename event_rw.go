package pipelines

import (
	"sync"
)

// Serves to pass Events to Handlers.
type EventReader[T any] interface {
	// Returns Event[T] channel.
	Read() <-chan Event[T]

	// Returns EventWriter instance on which this EventReader is based.
	GetWriter() EventWriter[T]
}

// Serves to write Events in Handle.Handle to chain Events.
type EventWriter[T any] interface {
	// Writes Event to a channel.
	Write(e Event[T])
	// Signals that no more writes are expected.
	// For internal use only!
	Done()
}

func newEventRW[T any](readers int) EventReader[T] {
	rw := &eventRW[T]{
		eventsChannel: make(chan Event[T], readers),
	}

	return rw
}

type eventRW[T any] struct {
	eventsChannel chan Event[T]

	shutdown     sync.Once
	writersGroup sync.WaitGroup
}

func (r *eventRW[T]) Read() <-chan Event[T] {
	return r.eventsChannel
}

func (r *eventRW[T]) GetWriter() EventWriter[T] {
	r.writersGroup.Add(1)

	r.shutdown.Do(func() {
		go func() {
			r.writersGroup.Wait()
			close(r.eventsChannel)
		}()
	})

	events := make(chan Event[T], 1)
	w := &eventW[T]{events: events}

	go func() {
		for e := range events {
			r.eventsChannel <- e
		}

		r.writersGroup.Done()
	}()

	return w
}

type eventW[T any] struct {
	once         sync.Once
	writeWGMutex sync.Mutex
	rwMu         sync.RWMutex
	writeWG      sync.WaitGroup

	events chan Event[T]
	isDone bool
}

func (r *eventW[T]) Write(e Event[T]) {
	r.writeWG.Add(1)
	go func() {
		r.rwMu.RLock()

		if !r.isDone {
			r.events <- e
		}

		r.rwMu.RUnlock()
		r.writeWG.Done()
	}()
}

func (r *eventW[T]) Done() {
	go r.once.Do(func() {
		r.writeWG.Wait()
		r.rwMu.Lock()

		r.isDone = true

		close(r.events)
		r.rwMu.Unlock()
	})
}
