package pipelines

import (
	"context"
)

func zero[T any]() T {
	var zero T
	return zero
}

// Handles single input and produces single output.
type Handle[T, U any] func(context.Context, T) (U, error)

// Constructs Handle from function, that has only error output.
func LiftErr[T any, Fn func(context.Context, T) error](fn Fn) Handle[T, T] {
	return func(ctx context.Context, v T) (T, error) {
		return v, fn(ctx, v)
	}
}

// Constructs Handle from function, that has no error output.
func LiftOk[T, U any, Fn func(context.Context, T) U](fn Fn) Handle[T, U] {
	return func(ctx context.Context, v T) (U, error) {
		return fn(ctx, v), nil
	}
}

// Constructs Handle from function, that has no context input.
func LiftNoContext[T, U any, Fn func(T) (U, error)](fn Fn) Handle[T, U] {
	return func(_ context.Context, v T) (U, error) {
		return fn(v)
	}
}

// Combines two Handles into one with input type T and output type N.
func Merge[T, U, N any, H1 Handle[T, U], H2 Handle[U, N]](h1 H1, h2 H2) Handle[T, N] {
	return func(ctx context.Context, payload T) (N, error) {
		v, err := h1(ctx, payload)
		if err != nil {
			return zero[N](), err
		}

		return h2(ctx, v)
	}
}

// Combines a Handle and error Handle into one with input type T and output type U.
func MergeErrHandle[T, U any, H Handle[T, U], ErrH Handle[error, U]](h H, errH ErrH) Handle[T, U] {
	return func(ctx context.Context, payload T) (U, error) {
		v, err := h(ctx, payload)
		if err != nil {
			return errH(ctx, NewError(err, payload))
		}

		return v, nil
	}
}
