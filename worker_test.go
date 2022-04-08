package pipelines_test

import (
	"context"
	"sync"
	"time"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/goleak"
)

var _ = Describe("Worker", func() {

	It("should start worker and handle events", func() {
		ctx := context.TODO()
		ctx, cancel := context.WithCancel(ctx)

		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
			r.Write(pipelines.Event[int]{Payload: 1})
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 1 + e})
		}
		handlerFunc3 := func(ctx context.Context, n int) (int, error) {
			return 1 + n, nil
		}

		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		var wg sync.WaitGroup
		eventSink := func(r *pipelines.Result[int]) {
			value, err := pipelines.Reduce(
				r,
				[]int{}, func(arr []int, next int) []int { return append(arr, next) },
				pipelines.NoError,
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal([]int{3, 3, 3, 3}))
			wg.Done()
		}

		w := pipelines.NewWorker(ctx, eventSink, c)

		wg.Add(1)
		go func() {
			err := w.Handle("start")

			Expect(err).ShouldNot(HaveOccurred())
		}()

		wg.Add(1)
		go func() {
			err := w.Handle("start")

			Expect(err).ShouldNot(HaveOccurred())
		}()

		wg.Wait()
		cancel()

		time.Sleep(time.Millisecond * 250)
		Eventually(w.IsRunning()).Should(BeFalse())

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

	It("should start worker and handle events", func() {
		ctx := context.TODO()
		ctx, cancel := context.WithCancel(ctx)

		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(pipelines.Event[int]{Payload: 1})
		}

		c := pipelines.New(handler1)

		var wg sync.WaitGroup
		eventSink := func(r *pipelines.Result[int]) {
			value, err := pipelines.Reduce(
				r,
				[]int{}, func(arr []int, next int) []int { return append(arr, next) },
				pipelines.NoError,
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(value).To(Equal([]int{1}))
			wg.Done()
		}

		w := pipelines.NewWorker(ctx, eventSink, c)

		cancel()
		time.Sleep(time.Millisecond * 250)

		err := w.Handle("start")

		Eventually(w.IsRunning()).Should(BeFalse())
		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError(pipelines.WorkerStopped))
	})
})
