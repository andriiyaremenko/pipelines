package pipelines_test

import (
	"context"
	"fmt"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Result", func() {
	ctx := context.TODO()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], command string) {
		if command == "produce error" {
			r.WriteError(pipelines.NewError(fmt.Errorf("error"), 1))
			r.WriteError(pipelines.NewError(fmt.Errorf("error"), 1))
			r.WriteError(pipelines.NewError(fmt.Errorf("error"), 1))
			r.WriteError(pipelines.NewError(fmt.Errorf("error"), 1))
			r.Write(1)

			return
		}

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

	Context("NoErrors", func() {
		It("should iterate through results using iterator", func() {
			count := 0
			for v, err := range c.Handle(ctx, "ok") {
				Expect(v).To(Equal(3))
				Expect(err).ShouldNot(HaveOccurred())

				count++
			}

			Expect(count).To(Equal(4))
		})

		It("Should work with break statement", func() {
			count := 0

			for v, err := range c.Handle(ctx, "ok") {
				Expect(v).To(Equal(3))
				Expect(err).ShouldNot(HaveOccurred())

				if count == 2 {
					break
				}

				count++
			}

			Expect(count).To(Equal(2))
		})

		It("Should be able to accumulate result", func() {
			aggregate := 0

			for v, err := range c.Handle(ctx, "ok") {
				Expect(v).To(Equal(3))
				Expect(err).ShouldNot(HaveOccurred())

				aggregate += v
			}

			Expect(aggregate).To(Equal(12))
		})
	})

	Context("Errors", func() {
		It("Should get errors", func() {
			countErrors := 0

			for _, err := range c.Handle(ctx, "produce error") {
				if err == nil {
					continue
				}

				Expect(err).Should(HaveOccurred())
				Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
				Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))

				countErrors++
			}

			Expect(countErrors).To(Equal(4))
		})

		It("Should work with break statement", func() {
			countErrors := 0

			for _, err := range c.Handle(ctx, "produce error") {
				if err == nil {
					continue
				}

				Expect(err).Should(HaveOccurred())
				Expect(err).Should(BeAssignableToTypeOf(new(pipelines.Error[int])))
				Expect(err.(*pipelines.Error[int]).Payload).To(Equal(1))
				if countErrors == 2 {
					break
				}

				countErrors++
			}

			Expect(countErrors).To(Equal(2))
		})
	})
})
