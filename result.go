package pipelines

import (
	"sync"

	"github.com/andriiyaremenko/pipelines/internal"
)

// Identity function of an error.
var NoError = func(err error) error {
	return err
}

// Skips errors, by calling skip callback.
func SkipErrors(skip func(error)) func(error) error {
	return func(err error) error {
		if err != nil {
			skip(err)
		}

		return nil
	}
}

// Interrupt will iterate through pipelines.Result until callback returns true.
func Interrupt[T any, Interrupt func(T, error) bool](result *Result[T], interrupt Interrupt) bool {
	defer result.close()

	for e := range result.Events {
		if interrupt(e.Payload, e.Err) {
			return true
		}
	}

	return false
}

// ForEach will iterate through pipelines.Result.
func ForEach[T any, Iterate func(int, T)](
	result *Result[T],
	iterate Iterate,
	handleErr func(error) error,
) error {
	defer result.close()

	i := 0
	for e := range result.Events {
		if e.Err == nil {
			iterate(i, e.Payload)

			i++

			continue
		}

		if err := handleErr(e.Err); err != nil {
			return err
		}
	}

	return nil
}

// Reduce will reduce pipelines.Result.
func Reduce[T, U any, Reduce func(U, T) U](
	result *Result[T],
	seed U,
	reduce Reduce,
	handleErr func(error) error,
) (U, error) {
	defer result.close()

	for e := range result.Events {
		if e.Err == nil {
			seed = reduce(seed, e.Payload)

			continue
		}

		if err := handleErr(e.Err); err != nil {
			return internal.Zero[U](), err
		}
	}

	return seed, nil
}

// Will collect only errors if there is any.
// Will exhaust the result.
func Errors[T any](result *Result[T]) []error {
	defer result.close()

	errors := make([]error, 0, 1)
	for e := range result.Events {
		if err := e.Err; err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// Will return on first encountered error or nil.
// Will exhaust the result.
func FirstError[T any](result *Result[T]) error {
	defer result.close()

	for e := range result.Events {
		if err := e.Err; err != nil {
			return err
		}
	}

	return nil
}

func newResult[T any](events <-chan Event[T], stop func()) *Result[T] {
	return &Result[T]{
		Events: events,
		stop:   stop,
	}
}

// Result returned by executing Pipeline
type Result[T any] struct {
	once sync.Once

	// Returns Event[T] channel to read events returned by executing Pipeline.
	Events <-chan Event[T]

	stop func()
}

func (r *Result[T]) close() {
	r.once.Do(func() {
		r.stop()

		for range r.Events {
		}
	})
}
