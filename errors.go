package pipelines

import (
	"fmt"
)

// Returns error with cause and payload.
func NewError[T any](cause error, payload T) error {
	return &Error[T]{cause: cause, Payload: payload}
}

type Error[T any] struct {
	cause error

	Payload T
}

func (err *Error[T]) Error() string {
	return fmt.Sprintf("error processing %T: %s", err.Payload, err.cause)
}

func (err *Error[T]) Unwrap() error {
	return err.cause
}
