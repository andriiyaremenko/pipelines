package pipelines_test

import (
	"context"
	"errors"
	"testing"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestPipeline(t *testing.T) {
	t.Run("CanCreatePipeline", CanCreatePipeline)
	t.Run("HandleChainedEvents", HandleChainedEvents)
	t.Run("HandleChainedEventsWithSeveralWrites", HandleChainedEventsWithSeveralWrites)
	t.Run("ShouldUseErrorHandlers", ShouldUseErrorHandlers)
	t.Run("ShouldShowErrorsInResult", ShouldShowErrorsInResult)
	t.Run("ParallelWorkers", ParallelWorkers)
	t.Run("GoroutinesLeaking", GoroutinesLeaking)
}

func CanCreatePipeline(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler := func(context.Context, string) (string, error) { return "", nil }
	c := pipelines.New[string, string](pipelines.HandleFunc(handler))

	suite.NoError(pipelines.FirstError(c.Handle(ctx, pipelines.Event[string]{})), "no error should be returned")
}

func HandleChainedEvents(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 42})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 42 + e.Payload})
	}
	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)

	value, err := pipelines.Reduce(
		c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
		pipelines.NoError(func(prev, next int) int { return prev + next }),
		0,
	)

	suite.NoError(err, "no error should be returned")
	suite.Equal(84, value)
}

func HandleChainedEventsWithSeveralWrites(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
	}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	value, err := pipelines.Reduce(
		c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
		pipelines.NoError(func(arr []int, next int) []int { return append(arr, next) }),
		[]int{},
	)
	suite.NoError(err, "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, value)
}

func ShouldUseErrorHandlers(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.NewErr[int](errors.New("some error")))
		r.Write(pipelines.Event[int]{Payload: 1})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
	}

	handlerFunc3 := func(ctx context.Context, n int) (int, error) {
		return 1 + n, nil
	}
	handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {}

	c := pipelines.Append[string, int, int](
		pipelines.New[string, int](handler1),
		pipelines.WithErrorHandler(handler2, handlerErr),
	)
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	value, err := pipelines.Reduce(
		c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
		pipelines.NoError(func(arr []int, next int) []int { return append(arr, next) }),
		[]int{},
	)
	suite.NoError(err, "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, value)
}

func ShouldShowErrorsInResult(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.NewErr[int](errors.New("some error")))
		r.Write(pipelines.Event[int]{Payload: 1})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
	}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	suite.Error(pipelines.FirstError(c.Handle(ctx, pipelines.Event[string]{Payload: "start"})), "error should be returned")
}

func ParallelWorkers(t *testing.T) {
	suite := assert.New(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
	}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, pipelines.WithWorkerPool(handler2, 4))
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	value, err := pipelines.Reduce(
		c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
		pipelines.NoError(func(arr []int, next int) []int { return append(arr, next) }),
		[]int{},
	)
	suite.NoError(err, "no error should be returned")
	suite.Equal([]int{3, 3, 3, 3}, value)
}

func GoroutinesLeaking(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[string]) {
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.Event[int]{Payload: 1})
		r.Write(pipelines.NewErr[int](errors.New("some error")))
		r.Write(pipelines.Event[int]{Payload: 1})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
		r.Write(pipelines.Event[int]{Payload: 1 + e.Payload})
	}
	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.New[string, int](handler1)
	c = pipelines.Append[string, int, int](c, handler2)
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	for i := 10; i > 0; i-- {
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
			pipelines.NoError(func(sum, next int) int { return sum + next }),
			0,
		)
		_, _ = pipelines.Reduce(
			c.Handle(ctx, pipelines.Event[string]{Payload: "start"}),
			pipelines.SkipErrors(func(sum, next int) int { return sum + next }, func(error) {}),
			0,
		)
		_ = pipelines.FirstError(c.Handle(ctx, pipelines.Event[string]{Payload: "start"}))
		_ = pipelines.Errors(c.Handle(ctx, pipelines.Event[string]{Payload: "start"}))
	}
}
