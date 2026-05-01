package storage

import (
	"context"
	"io"

	"github.com/zp001/ncp/pkg/model"
)

// Walker traverses a source and invokes fn for each discovered item.
type Walker interface {
	Walk(ctx context.Context, fn func(model.DiscoverItem) error) error
}

// Reader reads data from a source file at a given offset (pread semantics).
type Reader interface {
	io.ReaderAt
	io.Closer
}

// Source combines Walker and Reader creation for a storage backend.
type Source interface {
	Walker
	Open(relPath string) (Reader, error)
	Base() string
}
