package pipelines_test

import (
	"context"
	"errors"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/goleak"
)

var _ = Describe("Pipeline", func() {
	ctx := context.TODO()

	It("can create pipeline", func() {
		handler := func(context.Context, string) (string, error) { return "", nil }
		c := pipelines.New[string, string](pipelines.HandleFunc(handler))
		err := pipelines.FirstError(c.Handle(ctx, pipelines.Event[string]{}))

		Expect(err).ShouldNot(HaveOccurred())
	})

	It("can handle chained events", func() {
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

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal(84))
	})

	It("can handle chained events with several writes", func() {
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

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should use error handlers", func() {
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

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should show errors in result", func() {
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
		err := pipelines.FirstError(c.Handle(ctx, pipelines.Event[string]{Payload: "start"}))

		Expect(err).Should(HaveOccurred())
		Expect(err).Error().Should(Equal(errors.New("some error")))
	})

	It("should parallel work if worker pool was provided", func() {
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

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should not leak gourutines", func() {
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

		err := goleak.Find(
			goleak.
				IgnoreTopFunction(
					"github.com/onsi/ginkgo/v2/internal.(*Suite).runNode",
				),
			goleak.
				IgnoreTopFunction(
					"github.com/onsi/ginkgo/v2/internal/interrupt_handler.(*InterruptHandler).registerForInterrupts.func2",
				),
		)

		Expect(err).ShouldNot(HaveOccurred())
	})
})
