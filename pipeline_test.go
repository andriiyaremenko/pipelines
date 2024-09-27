package pipelines_test

import (
	"context"
	"fmt"
	"sync"
	"time"

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
		for _, err := range c.Handle(ctx, "") {
			Expect(err).ShouldNot(HaveOccurred())
		}
	})

	It("can handle chained events", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(42)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(42 + e)
		}
		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2)

		i := 0
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(84))
			i++
		}

		Expect(i).To(Equal(1))
	})

	It("can handle chained events with several writes", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2)
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		accumulated := []int{}
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(3))
			accumulated = append(accumulated, value)
		}

		Expect(accumulated).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should use error handlers", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.WriteError(fmt.Errorf("some error"))
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}

		handlerFunc3 := func(ctx context.Context, n int) (int, error) {
			return 1 + n, nil
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2, pipelines.WithErrorHandler(handlerErr))
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		accumulated := []int{}
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(3))
			accumulated = append(accumulated, value)
		}

		Expect(accumulated).To(Equal([]int{3, 3, 3, 3}))
	})

	It("should show errors in result", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.WriteError(fmt.Errorf("some error"))
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2)
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		errHappened := false
		for _, err := range c.Handle(ctx, "start") {
			if err != nil {
				errHappened = true
			}
		}

		Expect(errHappened).Should(BeTrue())
	})

	It("should parallel work if worker pool was provided", func() {
		var mu1, mu2 sync.Mutex

		locked := true
		mu1.Lock()

		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			defer r.Write(1 + e)
			// we want to lock only one goroutine
			mu2.Lock()
			if locked {
				locked = false

				defer mu1.Unlock()
				defer mu1.Lock()
			}
			mu2.Unlock()
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2, pipelines.WithHandlerPool[int](2))
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		i := 0
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(3))

			if i == 2 {
				mu1.Unlock()
			}

			i++
		}
	})

	It("should handle panic in handlers", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			panic("oh no...")
		}
		handleErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {
			Expect(e).Should(BeAssignableToTypeOf(new(pipelines.Error[string])))
			Expect(e.(*pipelines.Error[string]).Payload).To(Equal("start"))
			Expect(e.(*pipelines.Error[string]).Error()).To(Equal("error processing string: recovered from panic: oh no..."))
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.PipeErrorHandler(c, handleErr)
		c = pipelines.Pipe(c, handler2, pipelines.WithHandlerPool[int](4))
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		accumulated := []int{}
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(3))
			accumulated = append(accumulated, value)
		}

		Expect(accumulated).To(Equal([]int{3, 3, 3}))
	})

	It("should not leak gourutines", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.WriteError(fmt.Errorf("some error"))
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}
		handlerFunc3 := func(ctx context.Context, p int) (int, error) {
			return p + 1, nil
		}
		c := pipelines.New(handler1)
		c = pipelines.Pipe(c, handler2)
		c = pipelines.Pipe(c, pipelines.HandleFunc(handlerFunc3))

		for i := 10; i > 0; i-- {
			for value, err := range c.Handle(ctx, "start") {
				if err == nil {
					Expect(value).To(Equal(3))
				}
			}
			for value, err := range c.Handle(ctx, "start") {
				if err == nil {
					Expect(value).To(Equal(3))
				}
			}
			for value, err := range c.Handle(ctx, "start") {
				if err == nil {
					Expect(value).To(Equal(3))
				}
			}
			for value, err := range c.Handle(ctx, "start") {
				if err == nil {
					Expect(value).To(Equal(3))
				}
			}
			for value, err := range c.Handle(ctx, "start") {
				if err == nil {
					Expect(value).To(Equal(3))
				}
			}
		}

		time.Sleep(time.Millisecond * 250)

		err := goleak.Find(
			goleak.
				IgnoreTopFunction(
					"github.com/onsi/ginkgo/v2/internal.(*Suite).runNode",
				),
			goleak.
				IgnoreTopFunction(
					"github.com/onsi/ginkgo/v2/internal/interrupt_handler.(*InterruptHandler).registerForInterrupts.func2",
				),
			goleak.
				IgnoreAnyFunction(
					"github.com/onsi/ginkgo/v2/internal.RegisterForProgressSignal.func1",
				),
		)

		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should use options without overriding", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.WriteError(fmt.Errorf("some error"))
			r.Write(1)
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.Pipe(
			c,
			pipelines.PassThrough[int](),
			pipelines.WithErrorHandler(handlerErr),
			pipelines.WithHandlerPool[int](4),
		)

		accumulated := []int{}
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(1))
			accumulated = append(accumulated, value)
		}

		Expect(accumulated).To(Equal([]int{1, 1, 1, 1}))
	})

	It("should be able to append error handler", func() {
		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.WriteError(fmt.Errorf("some error"))
			r.Write(1)
		}
		handlerErr := func(ctx context.Context, r pipelines.EventWriter[int], e error) {}

		c := pipelines.New(handler1)
		c = pipelines.PipeErrorHandler(c, handlerErr)

		accumulated := []int{}
		for value, err := range c.Handle(ctx, "start") {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal(1))
			accumulated = append(accumulated, value)
		}

		Expect(accumulated).To(Equal([]int{1, 1, 1, 1}))
	})
})
