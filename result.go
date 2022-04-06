package pipelines

import "sync"

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
func Reduce[T, U any, Reduce Reducer[T, U]](result *Result[T], reduce Reduce, seed U) (U, error) {
	defer result.close()

	var err error
	for e := range result.Events {
		seed, err = reduce(seed, e.Payload, e.Err)
		if err != nil {
			return seed, err
		}
	}

	return seed, err
}

// Will collect only errors if there is any.
// Will exhaust result.
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
// Will exhaust result.
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
