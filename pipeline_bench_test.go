package pipelines_test

import (
	"context"
	"testing"

	"github.com/andriiyaremenko/pipelines"
)

func BenchmarkSingleHandler(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	c := pipelines.New[any, any](
		pipelines.HandleFunc(
			func(ctx context.Context, _ any) (any, error) {
				return nil, nil
			}),
	)

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkChainedEvent(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], e pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, handler2)

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkSeveralWrites(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, handler2)
	c = pipelines.Append[any, any, any](c, pipelines.HandleFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkParallelWorkers(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, pipelines.WithWorkerPool(handler2, 4))
	c = pipelines.Append[any, any, any](c, pipelines.HandleFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkSingleHandlerReduce(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	c := pipelines.New[any, any](
		pipelines.HandleFunc(
			func(ctx context.Context, _ any) (any, error) {
				return nil, nil
			}),
	)

	for i := 0; i < b.N; i++ {
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[any]{}),
			pipelines.NoError(func(_, _ any) any { return nil }),
			nil,
		)
	}
}

func BenchmarkChainedEventReduce(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], e pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, handler2)

	for i := 0; i < b.N; i++ {
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[any]{}),
			pipelines.NoError(func(_, _ any) any { return nil }),
			nil,
		)
	}
}

func BenchmarkSeveralWritesReduce(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, handler2)
	c = pipelines.Append[any, any, any](c, pipelines.HandleFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[any]{}),
			pipelines.NoError(func(_, _ any) any { return nil }),
			nil,
		)
	}
}

func BenchmarkParallelWorkersReduce(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.New[any, any](handler1)
	c = pipelines.Append[any, any, any](c, pipelines.WithWorkerPool(handler2, 4))
	c = pipelines.Append[any, any, any](c, pipelines.HandleFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[any]{}),
			pipelines.NoError(func(_, _ any) any { return nil }),
			nil,
		)
	}
}
