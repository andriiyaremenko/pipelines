package pipelines_test

import (
	"context"
	"iter"
	"testing"

	"github.com/andriiyaremenko/pipelines"
)

func BenchmarkWorkerWithMultipleWrites(b *testing.B) {
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
	}

	handlerFunc3 := func(ctx context.Context, n any) (any, error) {
		return nil, nil
	}

	c := pipelines.Pipe2(pipelines.Handler[any, any](handler1).Pipeline(), handler2, pipelines.HandleFunc(handlerFunc3))

	eventSink := func(result iter.Seq2[any, error]) {
		for range result {
		}
	}
	w := pipelines.NewWorker(ctx, eventSink, c)

	for i := 0; i < b.N; i++ {
		_ = w.Handle("start")
	}
}
