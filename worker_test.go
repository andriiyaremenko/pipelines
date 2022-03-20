package pipelines_test

import (
	"context"
	"sync"
	"testing"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/assert"
)

func TestCommandWorker(t *testing.T) {
	t.Run("Command Worker should start and Handle commands", testWorkerShouldStartAndHandleCommands)
}

func testWorkerShouldStartAndHandleCommands(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 1 + e.Payload()})
		}}

	handlerFunc3 := func(ctx context.Context, n int) (int, error) {
		return 1 + n, nil
	}

	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	var wg sync.WaitGroup
	eventSink := func(r pipelines.Result[int]) {
		assert.NoError(r.Err())
		assert.Equal([]int{3, 3, 3, 3}, r.Payload())
		wg.Done()
	}

	w := pipelines.NewWorker(ctx, eventSink, c)

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.E[string]{P: "start"})

		assert.NoError(err, "no error should be returned")
	}()

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.E[string]{P: "start"})

		assert.NoError(err, "no error should be returned")
	}()

	wg.Wait()
}
