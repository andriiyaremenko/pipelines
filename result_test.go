package pipelines_test

import (
	"context"
	"errors"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Result", func() {
	ctx := context.TODO()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], command string) {
		if command == "produce error" {
			r.Write(pipelines.NewErrEvent[int](pipelines.NewError(errors.New("error"), 1)))
			r.Write(pipelines.NewErrEvent[int](pipelines.NewError(errors.New("error"), 1)))
			r.Write(pipelines.NewErrEvent[int](pipelines.NewError(errors.New("error"), 1)))
			r.Write(pipelines.NewErrEvent[int](pipelines.NewError(errors.New("error"), 1)))
			r.Write(pipelines.Event[int]{Payload: 1})

			return
		}

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
	c := pipelines.New[string, int, pipelines.Handler[string, int]](handler1)
	c = pipelines.Append[string, int, int, pipelines.Handler[int, int]](c, handler2)
	c = pipelines.Append[string, int, int](c, pipelines.HandleFunc(handlerFunc3))

	Context("ForEach", func() {
		It("should iterate through results using iterator", func() {
			count := 0
			err := pipelines.ForEach(
				c.Handle(ctx, "ok"),
				func(i int, v int) {
					Expect(v).To(Equal(3))
					Expect(i).To(Equal(count))

					count++
				},
				pipelines.NoError,
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(count).To(Equal(4))
		})

		It("Should return first encountered error with NoError", func() {
			err := pipelines.ForEach(
				c.Handle(ctx, "produce error"),
				func(i int, v int) {},
				pipelines.NoError,
			)

			Expect(err).Should(HaveOccurred())
			Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
			Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))
		})

		It("Should skip errors with SkipError", func() {
			countErrors := 0
			err := pipelines.ForEach(
				c.Handle(ctx, "produce error"),
				func(i int, v int) {},
				pipelines.SkipErrors(func(err error) {
					Expect(err).Should(HaveOccurred())
					Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
					Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))

					countErrors++
				}),
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(countErrors).To(Equal(4))
		})
	})

	Context("Reduce", func() {
		It("Should reduce results using reducer", func() {
			sum, err := pipelines.Reduce(
				c.Handle(ctx, "ok"),
				0,
				func(sum int, v int) int {
					Expect(v).To(Equal(3))

					return sum + v
				},
				pipelines.NoError,
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(sum).To(Equal(4 * 3))
		})

		It("Should return first encountered error with NoError", func() {
			sum, err := pipelines.Reduce(
				c.Handle(ctx, "produce error"),
				0,
				func(sum int, v int) int {
					Expect(v).To(Equal(3))

					return sum + v
				},
				pipelines.NoError,
			)

			Expect(err).Should(HaveOccurred())
			Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
			Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))
			Expect(sum).To(Equal(0))
		})

		It("Should skip errors with SkipError", func() {
			countErrors := 0
			sum, err := pipelines.Reduce(
				c.Handle(ctx, "produce error"),
				0,
				func(sum int, v int) int {
					Expect(v).To(Equal(3))

					return sum + v
				},
				pipelines.SkipErrors(func(err error) {
					Expect(err).Should(HaveOccurred())
					Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
					Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))

					countErrors++
				}),
			)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(sum).To(Equal(1 * 3))
			Expect(countErrors).To(Equal(4))
		})
	})

	Context("Interrupt", func() {
		It("Should interrupt immediately after callback retuned true", func() {
			count := 0
			interrupted := pipelines.Interrupt(
				c.Handle(ctx, "ok"),
				func(int, error) bool {
					if count == 2 {
						return true
					}

					count++

					return false
				},
			)

			Expect(count).To(Equal(2))
			Expect(interrupted).To(BeTrue())
		})

		It("Should not interrupt if callback retuned false", func() {
			count := 0
			interrupted := pipelines.Interrupt(
				c.Handle(ctx, "ok"),
				func(int, error) bool {
					count++
					return false
				},
			)

			Expect(count).To(Equal(4))
			Expect(interrupted).To(BeFalse())
		})
	})
})
