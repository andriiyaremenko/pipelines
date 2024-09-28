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
			r.Write(42)
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(42 + e)
	}

	c := pipelines.New(handler1)
	c = pipelines.Pipe(c, handler2)
	for value, err := range c.Handle(ctx, "start") {
		// handle error
		if err != nil {
			// ...
		}

		// use result v:
		// ...
	}
}
```

#### Worker:

```go
import (
	"context"
    "iter"

	"github.com/andriiyaremenko/pipelines"
)
func main() {
	ctx := context.Background()

	handler1 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(42)
	}
	handler2 := func(ctx context.Context, r pipelines.EventWriter[int], e int) {
			r.Write(42 + e)
	}

	c := pipelines.New(handler1)
	c = pipelines.Pipe(c, handler2)

	eventSink := func(result iter.Seq2[int, error]) {
		for value, err := range result {
			// handle error
			if err != nil {
				// ...
			}

			// use result v:
			// ...
		}
	}

	w := pipelines.NewWorker(ctx, eventSink, c)
	err := w.Handle(0)

	// handle worker shut down error
	if err != nil {
		// ...
	}
}
```
