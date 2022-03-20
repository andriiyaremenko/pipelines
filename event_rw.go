package pipelines

import (
	"sync"
)

func newEventRW[T any]() EventReader[T] {
	rw := &eventRW[T]{
		eventsChannel: make(chan Event[T], 1),
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
	r.once.Do(func() {
		r.writeWG.Wait()
		r.rwMu.Lock()

		r.isDone = true

		close(r.events)
		r.rwMu.Unlock()
	})
}
