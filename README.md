# pipelines

This package helps a user create workflow pipelines.  

### To install pipelines:
```go
go get -u github.com/andriiyaremenko/pipelines
```

### How to use:
#### Pipeline:

```go
import (
	"context"

	"github.com/andriiyaremenko/pipelines"
)
func main() {
	ctx := context.Background()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e string) {
			r.Write(pipelines.Event[int]{Payload: 42})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.Event[int]{Payload: 42 + e})
	}

	c := pipelines.New[string, int, pipelines.Handler[string, int]](handler1)
	c = pipelines.Append[string, int, int, pipelines.Handler[int, int]](c, handler2)
	v, err := pipelines.Reduce(
		c.Handle(ctx, "start"),
		pipelines.NoError(func(sum, next int) int { return sum + next }),
		0,
	)

	// handle error
	if err != nil {
		// ...
	}

	// use result v:
	// ...
}
```

#### Worker:

```go
import (
	"context"

	"github.com/andriiyaremenko/pipelines"
)
func main() {
	ctx := context.Background()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.E[int]{P: 42})
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(pipelines.E[int]{P: 42 + e})
	}

	c := pipelines.New[int, int, pipelines.Handler[int, int]](handler1)
	c = pipelines.Append[string, int, int, pipelines.Handler[int, int]](c, handler2)

	eventSink := func(r *pipelines.Result[int]) {
		v, err := pipelines.Reduce(
			r,
			pipelines.NoError(func(sum, next int) int { return sum + next }),
			0,
		)

		// handle error
		if err != nil {
			// ...
		}

		// use result v:
		// ...
	}

	w := pipelines.NewWorker(ctx, eventSink, c)
	err := w.Handle(0)

	// handle worker shut down error
	if err != nil {
		// ...
	}
}
```
