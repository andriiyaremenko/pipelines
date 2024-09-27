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
			r.Write(1)
			r.Write(1)
			r.Write(1)
			r.Write(1)
		}
		handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(1 + e)
		}
		handlerFunc3 := func(ctx context.Context, n int) (int, error) {
			return 1 + n, nil
		}

		c := pipelines.New(handler1)
		c = pipelines.Append(c, handler2)
		c = pipelines.Append(c, pipelines.HandleFunc(handlerFunc3))

		var wg sync.WaitGroup
		eventSink := func(r *pipelines.Result[int]) {
			accumulated := []int{}
			for v, err := range pipelines.All(r) {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(v).To(Equal(3))
				accumulated = append(accumulated, v)
			}

			Expect(accumulated).To(Equal([]int{3, 3, 3, 3}))
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
			goleak.
				IgnoreAnyFunction(
					"github.com/onsi/ginkgo/v2/internal.RegisterForProgressSignal.func1",
				),
		)

		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should start worker and handle events", func() {
		ctx := context.TODO()
		ctx, cancel := context.WithCancel(ctx)

		handler1 := func(ctx context.Context, r pipelines.EventWriter[int], _ string) {
			r.Write(1)
		}

		c := pipelines.New(handler1)

		var wg sync.WaitGroup
		eventSink := func(r *pipelines.Result[int]) {
			accumulated := []int{}
			for v, err := range pipelines.All(r) {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(v).To(Equal(1))
				accumulated = append(accumulated, v)
			}

			Expect(accumulated).To(Equal([]int{1}))
			wg.Done()
		}

		w := pipelines.NewWorker(ctx, eventSink, c)

		cancel()
		time.Sleep(time.Millisecond * 250)

		err := w.Handle("start")

		Eventually(w.IsRunning()).Should(BeFalse())
		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError(pipelines.ErrWorkerStopped))
	})
})
