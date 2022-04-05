package pipelines

import "sync"

var _ Result[any] = new(result[any])

// Result returned by executing Pipeline
type Result[T any] interface {
	// Returns Event[T] channel to read events returned by executing Pipeline.
	Events() <-chan Event[T]

	// Terminates reading of the events.
	// Might take time to execute.
	// Should be called inside the same goroutine with Events().
	Close()
}

// Reducer function type to use in pipelines.Reduce.
type Reducer[T, U any] func(U, T, error) (U, error)

// Reducer function to use in pipelines.Reduce.
// Skips errors, by calling skip callback.
func SkipErrors[T, U any, Reduce func(U, T) U](reduce Reduce, skip func(error)) Reducer[T, U] {
	return func(agg U, next T, err error) (U, error) {
		if err != nil {
			skip(err)

			return agg, nil
		}

		return reduce(agg, next), nil
	}
}

// Reducer function to use in pipelines.Reduce.
// Returns on first encountered error.
func NoError[T, U any, Reduce func(U, T) U](reduce Reduce) Reducer[T, U] {
	return func(agg U, next T, err error) (U, error) {
		if err != nil {
			return agg, err
		}

		return reduce(agg, next), nil
	}
}

// Reducer function to process pipelines.Result.
// Will return on first error returned by Reducer callback.
func Reduce[T, U any, Reduce Reducer[T, U]](result Result[T], reduce Reduce, seed U) (U, error) {
	defer result.Close()

	var err error
	for e := range result.Events() {
		seed, err = reduce(seed, e.Payload, e.Err)
		if err != nil {
			return seed, err
		}
	}

	return seed, err
}

// Will collect only errors if there is any.
// Will exhaust result.
func Errors[T any](result Result[T]) []error {
	defer result.Close()

	errors := make([]error, 0, 1)
	for e := range result.Events() {
		if err := e.Err; err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// Will return on first encountered error or nil.
// Will exhaust result.
func FirstError[T any](result Result[T]) error {
	defer result.Close()

	for e := range result.Events() {
		if err := e.Err; err != nil {
			return err
		}
	}

	return nil
}

func newResult[T any](events <-chan Event[T], stop func()) *result[T] {
	return &result[T]{
		events: events,
		stop:   stop,
	}
}

type result[T any] struct {
	once sync.Once

	events <-chan Event[T]
	stop   func()
}

func (r *result[T]) Events() <-chan Event[T] {
	return r.events
}

func (r *result[T]) Close() {
	r.once.Do(func() {
		r.stop()

		for range r.events {
		}
	})
}
