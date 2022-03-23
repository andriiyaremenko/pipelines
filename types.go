package pipelines

import "context"

// Event carries information needed used in Pipeline execution.
type Event[T any] struct {
	Payload T
	Err     error
}

// Result returned by executing Pipeline
type Result[T any] interface {
	// Returns Event[T] channel to read events returned by executing Pipeline.
	Events() <-chan Event[T]

	// Terminates reading of the events.
	// Might take time to execute.
	// Should be called inside the same goroutine with Events().
	Close()
}

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

// Handles single type of Event.
type Handler[T, U any] interface {
	// Method to handle Events.
	Handle(ctx context.Context, w EventWriter[U], event Event[T])
	// Parallel workers
	Workers() int
}

// Handles Event and Event error of the same type.
type ErrorHandler[T, U any] interface {
	Handler[T, U]
	// Method to handle Event error.
	HandleError(ctx context.Context, w EventWriter[U], event Event[T])
}

// Combination of Handlers into one Pipeline
type Pipeline[T, U any] interface {
	// Handles initial Event and returns result of Pipeline execution.
	Handle(context.Context, Event[T]) Result[U]
	// Starts workers to handle incoming Event.
	Spin(context.Context) (EventWriter[T], EventReader[U])
}

// Asynchronous Pipeline
type Worker[T, U any] interface {
	// Asynchronously handles Event and returns error if Worker is stopped.
	Handle(Event[T]) error
	// returns false if Worker was stopped.
	IsRunning() bool
}
