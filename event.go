package pipelines

// Event carries information needed used in Pipeline execution.
type Event[T any] struct {
	Payload T
	Err     error
}
