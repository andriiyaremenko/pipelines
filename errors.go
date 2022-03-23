package pipelines

import (
	"fmt"

	"github.com/andriiyaremenko/pipelines/internal"
	"github.com/pkg/errors"
)

// Returns new Event[T] with error and zero value payload.
func NewErr[T any](err error) Event[T] {
	return Event[T]{Err: err}
}

// Returns new Event[T] with error and payload.
func NewErrEvent[T any](payload T, err error) Event[T] {
	return Event[T]{Payload: payload, Err: err}
}

// Returns new Event[T] with error containing information about handler and zero value payload.
func NewErrHandlerEvent[T, H any](handler H, err error) Event[T] {
	return Event[T]{
		Err: errors.Wrapf(err, fmt.Sprintf("%s failed", internal.InstanceTypeName(handler))),
	}
}
