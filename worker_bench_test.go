package pipelines_test

import (
	"context"
	"testing"

	"github.com/andriiyaremenko/pipelines"
)

func BenchmarkWorkerWithMultipleWrites(b *testing.B) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
		r.Write(pipelines.Event[any]{})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[any], _ any) {
		r.Write(pipelines.Event[any]{})
	}

	handlerFunc3 := func(ctx context.Context, n any) (any, error) {
		return nil, nil
	}

	c := pipelines.New[any, any, pipelines.Handler[any, any]](handler1)
	c = pipelines.Append[any, any, any, pipelines.Handler[any, any]](c, handler2)
	c = pipelines.Append[any, any, any](c, pipelines.HandleFunc(handlerFunc3))

	eventSink := func(r *pipelines.Result[any]) { _ = pipelines.FirstError(r) }
	w := pipelines.NewWorker(ctx, eventSink, c)

	for i := 0; i < b.N; i++ {
		_ = w.Handle(pipelines.Event[any]{Payload: "start"})
	}
}
