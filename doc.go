// This package is intended to help user create workflow pipelines.

// To install pipelines:
// 	go get -u github.com/andriiyaremenko/pipelines

// How to use:
//
// Pipeline:
// import (
// 	"context"
//
// 	"github.com/andriiyaremenko/pipelines"
// )
// func main() {
// 	ctx := context.Background()
//
// 	handler1 := &pipelines.BaseHandler[int, int]{
// 		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
// 			r.Write(pipelines.E[int]{P: 42})
// 		}}
// 	handler2 := &pipelines.BaseHandler[int, int]{
// 		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
// 			r.Write(pipelines.E[int]{P: 42 + e.Payload()})
// 		}}
//
// 	c := pipelines.New[string, int](handler1)
// 	c = pipelines.Append[string, int, int](c, handler2)
// 	ev := c.Handle(ctx, pipelines.E[int]{P: 0})
//
// 	// handle error
// 	if err := ev.Err(); err != nil {
// 		// ...
// 	}
//
// 	// or every underlying error separately:
// 	for _, err := range ev.Errors() {
// 		// ...
// 	}
//
// 	// use result:
// 	elements := ev.Payload()
// }
//
// Worker:
// import (
// 	"context"
//
// 	"github.com/andriiyaremenko/pipelines"
// )
// func main() {
// 	ctx := context.Background()
//
// 	handler1 := &pipelines.BaseHandler[int, int]{
// 		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
// 			r.Write(pipelines.E[int]{P: 42})
// 		}}
// 	handler2 := &pipelines.BaseHandler[int, int]{
// 		HandleFunc: func(ctx context.Context, r pipelines.EventWriter[int], e pipelines.Event[int]) {
// 			r.Write(pipelines.E[int]{P: 42 + e.Payload()})
// 		}}
//
// 	c := pipelines.New[string, int](handler1)
// 	c = pipelines.Append[string, int, int](c, handler2)
//
// 	eventSink := func(ev pipelines.Result[int]) {
// 		// handle error
// 		if err := ev.Err(); err != nil {
// 			// ...
// 		}
//
// 		// or every underlying error separately:
// 		for _, err := range ev.Errors() {
// 			// ...
// 		}
//
// 		// use result:
// 		elements := ev.Payload()
// 	}
//
// 	w := pipelines.NewWorker(ctx, eventSink, c)
// 	err := w.Handle(pipelines.E[int]{P: 0})
//
// 	// handle worker shut down error
// }
package pipelines
