package query

import (
	"context"
)

type Handler interface {
	QueryName() string
	Handle(ctx context.Context, payload []byte) ([]byte, error)
}

type Demultiplexer interface {
	Handle(ctx context.Context, query string, payload []byte) ([]byte, error)
}
