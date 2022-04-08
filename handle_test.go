package pipelines_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/andriiyaremenko/pipelines"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handle", func() {
	It("can lift error function", func() {
		fn := pipelines.LiftErr(func(context.Context, int) error { return nil })
		v, err := fn(context.TODO(), 0)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal(0))

		fn = pipelines.LiftErr(func(context.Context, int) error { return errors.New("failed") })
		v, err = fn(context.TODO(), 0)

		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError("failed"))
	})

	It("can lift value function", func() {
		fn := pipelines.LiftOk(func(_ context.Context, n int) int { return n + 1 })
		v, err := fn(context.TODO(), 1)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal(2))
	})

	It("can lift function without context", func() {
		fn := pipelines.LiftNoContext(func(n int) (int, error) { return n + 1, nil })
		v, err := fn(context.TODO(), 1)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal(2))

		fn = pipelines.LiftNoContext(func(n int) (int, error) { return 0, errors.New("failed") })
		v, err = fn(context.TODO(), 1)

		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError("failed"))
	})

	It("can append handle", func() {
		_fn := pipelines.AppendHandle(
			pipelines.LiftNoContext(func(n int) (int, error) { return n + n, nil }),
			pipelines.LiftNoContext(func(n int) (int, error) { return n * n, nil }),
		)
		fn := pipelines.AppendHandle(
			_fn,
			pipelines.LiftNoContext(func(n int) (string, error) { return fmt.Sprintf("got %d", n), nil }),
		)
		v, err := fn(context.TODO(), 1)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal("got 4"))

		errFn := pipelines.AppendHandle(
			pipelines.LiftNoContext(func(n int) (int, error) { return 0, errors.New("failed") }),
			fn,
		)

		v, err = errFn(context.TODO(), 1)

		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError("failed"))
	})

	It("can append error handle", func() {
		_fn := pipelines.AppendHandle(
			pipelines.LiftNoContext(func(n int) (int, error) { return n + n, nil }),
			pipelines.LiftNoContext(func(n int) (int, error) { return n * n, nil }),
		)
		_errFn := pipelines.AppendHandle(
			_fn,
			pipelines.LiftNoContext(func(n int) (int, error) { return 0, errors.New("failed") }),
		)
		_fn1 := pipelines.AppendErrHandle(
			_errFn,
			pipelines.LiftNoContext(func(err error) (int, error) { return err.(*pipelines.Error[int]).Payload, nil }),
		)
		fn := pipelines.AppendHandle(
			_fn1,
			pipelines.LiftNoContext(func(n int) (string, error) { return fmt.Sprintf("got %d", n), nil }),
		)
		v, err := fn(context.TODO(), 1)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal("got 1"))

		_fn2 := pipelines.AppendErrHandle(
			_errFn,
			pipelines.LiftNoContext(func(err error) (int, error) { return 0, fmt.Errorf("wrapped: %s", err) }),
		)
		fn = pipelines.AppendHandle(
			_fn2,
			pipelines.LiftNoContext(func(n int) (string, error) { return fmt.Sprintf("got %d", n), nil }),
		)
		v, err = fn(context.TODO(), 1)

		Expect(err).Should(HaveOccurred())
		Expect(err).Should(MatchError("wrapped: error processing int: failed"))

		_fn3 := pipelines.AppendErrHandle(
			_fn,
			pipelines.LiftNoContext(func(err error) (int, error) { return 0, nil }),
		)
		fn = pipelines.AppendHandle(
			_fn3,
			pipelines.LiftNoContext(func(n int) (string, error) { return fmt.Sprintf("got %d", n), nil }),
		)
		v, err = fn(context.TODO(), 1)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(v).To(Equal("got 4"))
	})
})
