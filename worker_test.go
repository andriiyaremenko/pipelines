package pipelines_test

import (
	"context"
	"sync"
	"testing"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/suite"
)

func TestWorker(t *testing.T) {
	suite.Run(t, new(workerSuite))
}

type workerSuite struct {
	suite.Suite
}

func (suite *workerSuite) TestShouldStartAndHandleEvents() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

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
		suite.NoError(r.Err())
		suite.Equal([]int{3, 3, 3, 3}, r.Payload())
		wg.Done()
	}

	w := pipelines.NewWorker(ctx, eventSink, c)

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.E[string]{P: "start"})

		suite.NoError(err, "no error should be returned")
	}()

	wg.Add(1)
	go func() {
		err := w.Handle(pipelines.E[string]{P: "start"})

		suite.NoError(err, "no error should be returned")
	}()

	wg.Wait()
}
