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

	c := pipelines.
		HandleFunc(
			func(ctx context.Context, _ any) (any, error) {
				return nil, nil
			},
		).
		Pipeline()

	for i := 0; i < b.N; i++ {
		for range c.Handle(ctx, nil) {
		}
	}
}

func BenchmarkChainedEvent(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(struct{}{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], e any) {
		r.Write(struct{}{})
	}

	c := pipelines.Pipe(pipelines.Handler[any, any](handler1).Pipeline(), handler2)

	for i := 0; i < b.N; i++ {
		for range c.Handle(ctx, struct{}{}) {
		}
	}
}

func BenchmarkSeveralWrites(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.Pipe2(pipelines.Handler[any, any](handler1).Pipeline(), handler2, pipelines.HandleFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		for range c.Handle(ctx, struct{}{}) {
		}
	}
}

func BenchmarkParallelWorkers(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
		r.Write(struct{}{})
	}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.Pipe2(pipelines.Handler[any, any](handler1).Pipeline(), handler2, pipelines.HandleFunc(handlerFunc3), pipelines.WithHandlerPool(4))

	for i := 0; i < b.N; i++ {
		for range c.Handle(ctx, struct{}{}) {
		}
	}
}
