package pipelines_test

import (
	"context"
	"errors"
	"testing"

	"github.com/andriiyaremenko/pipelines"
	"github.com/stretchr/testify/assert"
)

func TestCommand(t *testing.T) {
	t.Run("Should be able to create command and handle command", testCanCreateCommand)
	t.Run("Command should be able to chain events", testCommandHandleChainEvents)
	t.Run("Command should handle nil", testCommandHandleNil)
	t.Run("Command should be able to chain events and handle several events from one handler",
		testCommandHandleChainEventsSeveralEvents)
	t.Run("Command should be able to chain events and use registered error handlers",
		testCommandHandleChainEventsShouldUseErrorHandlers)
	t.Run("Command should be able to show unhandled errors in result",
		testShouldShowErrorsInResult)
}

func testCanCreateCommand(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
	handler := func(ctx context.Context, _ string) (string, error) {
		return "", nil
	}
	c := pipelines.New(
		pipelines.HandlerFunc(handler),
	)

	assert.NoError(c.Handle(ctx, pipelines.E[string]{}).Err(), "no error should be returned")
}

func testCommandHandleChainEvents(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
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

	assert.NoError(ev.Err(), "no error should be returned")
	assert.Equal([]int{84}, ev.Payload())
}

func testCommandHandleNil(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
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
	assert.Error(ev.Err(), "error should be returned")
}

func testCommandHandleChainEventsSeveralEvents(t *testing.T) {
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

	handlerFunc3 := func(ctx context.Context, p int) (int, error) {
		return p + 1, nil
	}
	c := pipelines.Append[string, int, int](pipelines.New[string, int](handler1), handler2)
	c = pipelines.Append(c, pipelines.HandlerFunc(handlerFunc3))

	ev := c.Handle(ctx, pipelines.E[string]{P: "start"})
	assert.NoError(ev.Err(), "no error should be returned")
	assert.Equal([]int{3, 3, 3, 3}, ev.Payload())
}

func testCommandHandleChainEventsShouldUseErrorHandlers(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
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
	assert.NoError(ev.Err(), "no error should be returned")
	assert.Equal([]int{3, 3, 3, 3}, ev.Payload())
}

func testShouldShowErrorsInResult(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	assert := assert.New(t)
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
	assert.Error(ev.Err(), "error should be returned")
}
