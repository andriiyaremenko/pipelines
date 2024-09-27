package pipelines

import (
	"iter"
	"sync"

	"github.com/andriiyaremenko/pipelines/internal"
)

// All will iterate through pipelines.Result.
func All[T any](result *Result[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		defer result.close()

		for e := range result.Events {
			if !yield(e.Payload, e.Err) {
				return
			}
		}
	}
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
func Errors[T any](result *Result[T]) iter.Seq[error] {
	return func(yield func(error) bool) {
		defer result.close()

		for e := range result.Events {
			if e.Err != nil {
				if !yield(e.Err) {
					return
				}
			}
		}
	}
}

func newResult[T any](events <-chan Event[T], stop func()) *Result[T] {
	return &Result[T]{
		Events: events,
		stop:   stop,
	}
}

// Result returned by executing Pipeline
type Result[T any] struct {
	Events <-chan Event[T]
	stop   func()
	once   sync.Once
}

func (r *Result[T]) close() {
	r.once.Do(func() {
		r.stop()

		for range r.Events {
		}
	})
}
