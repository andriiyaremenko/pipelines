package pipelines_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestWorker(t *testing.T) {
	t.Run("ShouldStartAndHandleEvents", WorkerShouldStartAndHandleEvents)
}

func WorkerShouldStartAndHandleEvents(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
		}}
	handlerFunc3 := func(ctx context.Context, n int) (int, error) {
		return 1 + n, nil
	}

	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	var wg sync.WaitGroup
	eventSink := func(r pipelines.Result[int]) {
		value, err := pipelines.Reduce(
			r,
			pipelines.NoError(func(arr []int, next int) []int { return append(arr, next) }),
			[]int{},
		)

		suite.NoError(err)
		suite.Equal([]int{3, 3, 3, 3}, value)
		wg.Done()
	}

	w := pipelines.NewWorker(ctx, eventSink, c)

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.Event[string]{Payload: "start"})

		suite.NoError(err, "no error should be returned")
	}()

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.Event[string]{Payload: "start"})

		suite.NoError(err, "no error should be returned")
	}()

	wg.Wait()
	cancel()

	time.Sleep(time.Millisecond * 250)
	suite.False(w.IsRunning())
}
