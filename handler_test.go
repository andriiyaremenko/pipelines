package pipelines_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type TestWriter[T any] func(event pipelines.Event[T])

func (writer TestWriter[T]) Write(event T) {
	writer(pipelines.Event[T]{Payload: event})
}

func (writer TestWriter[T]) WriteError(err error) {
	writer(pipelines.Event[T]{Err: err})
}

var _ = Describe("Handler", func() {
	It("HandleFunc will write success", func() {
		fn := pipelines.HandleFunc(
			pipelines.LiftOk(func(_ context.Context, n int) int { return n + 1 }),
		)

		var w TestWriter[int] = func(event pipelines.Event[int]) {
			Expect(event.Err).ShouldNot(HaveOccurred())
			Expect(event.Payload).To(Equal(2))
		}

		fn(context.TODO(), w, 1)
	})

	It("HandlerFunc will write error", func() {
		fn := pipelines.HandleFunc(
			pipelines.LiftErr(func(_ context.Context, n int) error { return fmt.Errorf("failed") }),
		)

		var w TestWriter[int] = func(event pipelines.Event[int]) {
			Expect(event.Err).Should(HaveOccurred())
			Expect(event.Err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
			Expect(event.Err).To(MatchError("error processing int: failed"))
			Expect(event.Err.(*pipelines.Error[int]).Payload).To(Equal(1))
			Expect(errors.Unwrap(event.Err)).To(MatchError("failed"))
		}

		fn(context.TODO(), w, 1)
	})
})
