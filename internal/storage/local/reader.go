package local

import "os"

// Reader wraps os.File and provides ReadAt (pread) semantics.
type Reader struct {
	f *os.File
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	return r.f.ReadAt(p, off)
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.f.Close()
}

// Fd returns the underlying file descriptor (for sendfile optimization).
func (r *Reader) Fd() uintptr {
	return r.f.Fd()
}
