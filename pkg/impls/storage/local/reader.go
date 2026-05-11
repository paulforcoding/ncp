package local

import (
	"context"
	"os"

	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// Reader wraps os.File and implements storage.FileReader.
type Reader struct {
	f    *os.File
	size int64
	attr storage.FileAttr
}

// Read reads up to len(p) bytes from the file.
func (r *Reader) Read(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	return r.f.Read(p)
}

// Close closes the underlying file.
func (r *Reader) Close(ctx context.Context) error {
	return r.f.Close()
}

// Size returns the file size.
func (r *Reader) Size() int64 { return r.size }

// Attr returns the file attributes.
func (r *Reader) Attr() storage.FileAttr { return r.attr }

// Fd returns the underlying file descriptor (for sendfile optimization).
func (r *Reader) Fd() uintptr {
	return r.f.Fd()
}
