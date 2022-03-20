package pipelines

import (
	"fmt"
	"strings"
	"sync"

	"github.com/andriiyaremenko/pipelines/internal"
)

var (
	_ Event[any] = new(ErrEvent[any])
	_ error      = new(ErrEvent[any])
	_ Event[any] = new(ErrHandlerEvent[any])
	_ error      = new(ErrHandlerEvent[any])
	_ error      = new(ErrAggregated)
)

// Returns new *ErrEvent[T] caused by event.
// *ErrEvent implements error and Event[T].
func NewErrEvent[T any](err error) *ErrEvent[T] {
	return &ErrEvent[T]{err}
}

type ErrEvent[T any] struct {
	cause error
}

// Returns zero value of type T.
func (err *ErrEvent[T]) Payload() T {
	return internal.ZeroValue[T]()
}

// Returns the underlying error.
func (err *ErrEvent[T]) Err() error {
	return err.cause
}

// Implementation of error.
func (err *ErrEvent[T]) Error() string {
	return err.cause.Error()
}

// Returns underlying error.
func (err *ErrEvent[T]) Unwrap() error {
	return err.cause
}

// Returns new *ErrHandlerEvent caused by event.
// *ErrHandlerEvent implements error and Event.
func NewErrHandlerEvent[T, H any](handler H, err error) *ErrHandlerEvent[T] {
	return &ErrHandlerEvent[T]{err, internal.InstanceTypeName(handler)}
}

type ErrHandlerEvent[T any] struct {
	cause       error
	handlerName string
}

// Returns zero value of type T.
func (err *ErrHandlerEvent[T]) Payload() T {
	return internal.ZeroValue[T]()
}

// Returns *ErrHandlerEvent[T] as an error.
func (err *ErrHandlerEvent[T]) Err() error {
	return err
}

// Implementation of error.
func (err *ErrHandlerEvent[T]) Error() string {
	return fmt.Sprintf("%s failed: %s", err.handlerName, err.Unwrap())
}

// Returns underlying error.
func (err *ErrHandlerEvent[T]) Unwrap() error {
	return err.cause
}

// Returns new *ErrAggregatedEvent caused by event or Events dispatched while processing event.
// *ErrAggregatedEvent implements error and Event.
func NewErrAggregated(eventName string) *ErrAggregated {
	return &ErrAggregated{eventName: eventName}
}

type ErrAggregated struct {
	mu sync.RWMutex

	errors    []error
	eventName string
}

// Returns *ErrAggregatedEvent as an error.
func (err *ErrAggregated) Err() error {
	err.mu.RLock()
	defer err.mu.RUnlock()

	if len(err.errors) == 0 {
		return nil
	}

	var sb strings.Builder

	sb.WriteByte('\n')

	for _, e := range err.errors {
		sb.WriteByte('\t')
		sb.WriteString(e.Error())
		sb.WriteByte('\n')
	}

	return fmt.Errorf(
		"failed to process event %s: aggregated error occurred: [%s]",
		err.eventName, sb.String())
}

// Implementation of error.
func (err *ErrAggregated) Error() string {
	return err.Err().Error()
}

// Returns list of all errors caused by processing initial Event.
// or Events dispatched while processing this Event.
func (err *ErrAggregated) Inner() []error {
	err.mu.RLock()
	defer err.mu.RUnlock()
	return err.errors
}

// Appends errors to *ErrAggregatedEvent error list.
func (err *ErrAggregated) Append(errors ...error) {
	err.mu.Lock()
	defer err.mu.Unlock()
	err.errors = append(err.errors, errors...)
}

// error type returned if Event equals nil.
type ErrNilEvent[T any] string

// Implementation of error.
func (err ErrNilEvent[T]) Error() string {
	return fmt.Sprintf(
		"%s: got %s event with value of nil",
		string(err),
		internal.TypeName[T](),
	)
}
