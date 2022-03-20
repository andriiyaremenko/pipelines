package pipelines

import (
	"sync"
)

var (
	_ Event[any]  = E[any]{}
	_ Event[any]  = new(E[any])
	_ Result[any] = new(result[any])
)

// E implements Event.
type E[T any] struct {
	P T
}

func (e E[T]) Payload() T {
	return e.P
}

func (e E[T]) Err() error {
	return nil
}

func newResult[T any](eventType string) *result[T] {
	return &result[T]{errors: NewErrAggregated(eventType)}
}

type result[T any] struct {
	mu sync.Mutex

	results []T
	errors  *ErrAggregated
}

func (r *result[T]) Errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.errors.Inner()
}

func (r *result[T]) Append(ev Event[T]) {
	r.mu.Lock()

	r.results = append(r.results, ev.Payload())

	r.mu.Unlock()
}

func (r *result[T]) AppendError(err error) {
	r.mu.Lock()
	r.errors.Append(err)
	r.mu.Unlock()
}

func (r *result[T]) Payload() []T {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.results
}

func (r *result[T]) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.errors.Err() == nil {
		return nil
	}

	return r.errors
}
