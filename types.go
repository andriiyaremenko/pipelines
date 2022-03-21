package pipelines

import "context"

// Event carries information needed to execute Commands.Handle and Commands.HandleOnly.
type Event[T any] interface {
	// Information needed to process Event or returned by executing Event.
	Payload() T
	// Error caused by executing Event.
	Err() error
}

type Result[T any] interface {
	Event[[]T]

	Errors() []error
}

// Serves to pass Events to Handlers.
type EventReader[T any] interface {
	// Returns EventWithMetadata channel.
	Read() <-chan Event[T]

	// Returns EventWriter instance on which this EventReader is based.
	GetWriter() EventWriter[T]
}

// Serves to write Events in Handle.Handle to chain Events.
// Do NOT forget to call Done() when finished writing.
type EventWriter[T any] interface {
	// Writes Event to a channel.
	Write(e Event[T])
	// Signals Commands that Handler is done writing.
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
