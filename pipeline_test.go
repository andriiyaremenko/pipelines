package pipelines_test

import (
	"context"
	"errors"
	"sync"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/goleak"
)

var _ = Describe("Pipeline", func() {
	ctx := context.TODO()

	It("can create pipeline", func() {
		handler := func(context.Context, string) (string, error) { return "", nil }
		c := pipelines.New(pipelines.HandleFunc(handler))
		err := pipelines.FirstError(c.Handle(ctx, ""))

		Expect(err).ShouldNot(HaveOccurred())
	})

	It("can handle chained events", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 42})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 42 + e})
		}
		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			0, func(prev, next int) int { return prev + next },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal(84))
	})

	It("can handle chained events with several writes", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			[]int{}, func(arr []int, next int) []int { return append(arr, next) },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should use error handlers", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.NewErrEvent[int](errors.New("some error")))
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}

		handlerFunc3 := func(ctx context.Context, n int) (int, error) {
			return 1 + n, nil
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2, pipelines.WithErrorHandler(handlerErr))
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			[]int{}, func(arr []int, next int) []int { return append(arr, next) },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should show errors in result", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.NewErrEvent[int](errors.New("some error")))
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))
		err := pipelines.FirstError(c.Handle(ctx, "start"))

		Expect(err).Should(HaveOccurred())
		Expect(err).Error().Should(Equal(errors.New("some error")))
	})

	It("should parallel work if worker pool was provided", func() {
		mu := new(sync.Mutex)
		once := new(sync.Once)
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			once.Do(mu.Lock)
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2, pipelines.WithHandlerPool[int](2))
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		err := pipelines.ForEach(
			c.Handle(ctx, "start"),
			func(i int, next int) {
				Expect(next).To(Equal(3))

				if i == 2 {
					mu.Unlock()
				}
			},
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should handle panic in handlers", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			panic("oh no...")
		}
		handleErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {
			Expect(e).Should(BeAssignableToTypeOf(new(pipelines.Error[any])))
			Expect(e.(*pipelines.Error[any]).Payload).To(Equal("oh no..."))
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.AppendErrorHandler(c, handleErr)
		c = pipelines.Append(c, handler2, pipelines.WithHandlerPool[int](4))
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			[]int{}, func(arr []int, next int) []int { return append(arr, next) },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{3, 3, 3}))
	})

	It("should not leak gourutines", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.NewErrEvent[int](errors.New("some error")))
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		for i := 10; i > 0; i-- {
			_, _ = pipelines.Reduce(
				c.Handle(ctx, "start"),
				0, func(sum, next int) int { return sum + next },
				pipelines.NoError,
			)
			_, _ = pipelines.Reduce(
				c.Handle(ctx, "start"),
				0, func(sum, next int) int { return sum + next },
				pipelines.SkipErrors(func(error) {}),
			)
			_ = pipelines.FirstError(c.Handle(ctx, "start"))
			_ = pipelines.Errors(c.Handle(ctx, "start"))
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

	It("should use options without overriding", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.NewErrEvent[int](errors.New("some error")))
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.Append(
			c,
			pipelines.PassThrough[int](),
			pipelines.WithErrorHandler(handlerErr),
			pipelines.WithHandlerPool[int](4),
		)

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			[]int{}, func(arr []int, next int) []int { return append(arr, next) },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{1, 1, 1, 1}))
	})

	It("should be able to append error handler", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.NewErrEvent[int](errors.New("some error")))
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.AppendErrorHandler(c, handlerErr)

		value, err := pipelines.Reduce(
			c.Handle(ctx, "start"),
			[]int{}, func(arr []int, next int) []int { return append(arr, next) },
			pipelines.NoError,
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(value).To(Equal([]int{1, 1, 1, 1}))
	})
})
