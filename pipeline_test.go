package pipelines_test

import (
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/suite"
)

func TestPipeline(t *testing.T) {
	suite.Run(t, new(pipelineSuite))
}

type pipelineSuite struct {
	suite.Suite
}

func (suite *pipelineSuite) TestCanCreatePipeline() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler := func(ctx context.Context, _ string) (string, error) {
		return "", nil
	}
	c := pipelines.New(
		pipelines.HandlerFunc(handler),
	)

	suite.NoError(c.Handle(ctx, pipelines.E[string]{}).Err(), "no error should be returned")

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestHandleChainedEvents() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 42})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 42 + e.Payload()})
		}}

	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})

	suite.NoError(ev.Err(), "no error should be returned")
	suite.Equal([]int{84}, ev.Payload())

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestHandleNilEvent() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 42})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 42 + e.Payload()})
		}}

	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)

	ev := c.Handle(ctx, nil)
	suite.Error(ev.Err(), "error should be returned")

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestHandleChainedEventsWithSeveralWrites() {
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

	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	suite.NoError(ev.Err(), "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, ev.Payload())

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestShouldUseErrorHandlers() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.NewErrHandlerEvent[int]("FirstHandler", errors.New("some error")))
			r.Write(pipelines.E[int]{P: 1})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 1 + e.Payload()})
		}}

	handlerFunc3 := func(ctx context.Context, n int) (int, error) {
		return 1 + n, nil
	}

	handlerErr := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {},
	}
	c := pipelines.Append[string, int, int](
		pipelines.New[string, int](handler1),
		pipelines.WithErrorHandler[int, int](handler2, handlerErr),
	)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	suite.NoError(ev.Err(), "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, ev.Payload())

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestShouldShowErrorsInResult() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.NewErrHandlerEvent[int]("FirstHandler", errors.New("some error")))
			r.Write(pipelines.E[int]{P: 1})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 1 + e.Payload()})
		}}

	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	suite.Error(ev.Err(), "error should be returned")

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestParallelWorkers() {
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
		NWorkers: 4,
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 1 + e.Payload()})
		}}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}

	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	suite.NoError(ev.Err(), "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, ev.Payload())

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

func (suite *pipelineSuite) TestWriteNilEvent() {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := &pipelines.BaseHandler[string, int]{
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
			r.Write(pipelines.E[int]{P: 1})
			r.Write(pipelines.E[int]{P: 1})
			r.Write(nil)
			r.Write(pipelines.E[int]{P: 1})
		}}
	handler2 := &pipelines.BaseHandler[int, int]{
		NWorkers: 4,
		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
			r.Write(pipelines.E[int]{P: 1 + e.Payload()})
		}}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}

	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	suite.Error(ev.Err(), "error should be returned")
	suite.Equal([]int{3, 3, 3}, ev.Payload())

	hangingGoroutines := runtime.NumGoroutine() - 3
	if hangingGoroutines != 0 {
		suite.Failf("leaky goroutines", "%d leaky goroutines found", hangingGoroutines)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}
