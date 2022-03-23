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

	handler := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}
	c := pipelines.New(
		pipelines.HandlerFunc(handler),
	)

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkChainedEvent(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[any, any]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
		}}
	handler2 := &pipelines.BaseHandler[any, any]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], e pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
		}}

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

	handler1 := &pipelines.BaseHandler[any, any]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
		}}
	handler2 := &pipelines.BaseHandler[any, any]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
		}}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.Append[any, any, any](pipelines.New[any, any](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}

func BenchmarkParallelWorkers(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[any, any]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
		}}
	handler2 := &pipelines.BaseHandler[any, any]{
		NWorkers: 4,
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[any], _ pipelines.Event[any]) {
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
			r.Write(pipelines.Event[any]{})
		}}
	handlerFunc3 := func(ctx context.Context, _ any) (any, error) {
		return nil, nil
	}

	c := pipelines.Append[any, any, any](pipelines.New[any, any](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	for i := 0; i < b.N; i++ {
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[any]{}))
	}
}
