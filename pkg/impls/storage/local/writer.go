package local

import (
	"context"
	"fmt"
	"os"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// WriterConfig controls IO behavior for local file writes.
type WriterConfig struct {
	DirectIO    bool
	SyncWrites  bool
	IOSize      int // 0 = use tiered
	IOSizeTiers []model.IOSizeTier
}

// DefaultWriterConfig returns the default writer config.
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		SyncWrites:  true,
		IOSizeTiers: model.DefaultIOSizeTiers(),
	}
}

// Writer wraps os.File and implements storage.FileWriter.
type Writer struct {
	f            *os.File
	cfg          WriterConfig
	size         int64
	bytesWritten int64
	committed    bool
	aborted      bool
}

var _ storage.FileWriter = (*Writer)(nil)

// Write writes len(p) bytes to the file.
func (w *Writer) Write(_ context.Context, p []byte) (int, error) {
	if w.committed || w.aborted {
		return 0, fmt.Errorf("local: write on closed writer")
	}
	n, err := w.f.Write(p)
	if n > 0 {
		w.bytesWritten += int64(n)
	}
	return n, err
}

// Commit closes the file. SyncWrites triggers a final fsync before close.
// The checksum parameter is ignored for local copies.
func (w *Writer) Commit(_ context.Context, _ []byte) error {
	if w.committed || w.aborted {
		return nil
	}
	w.committed = true
	var syncErr error
	if w.cfg.SyncWrites {
		syncErr = w.f.Sync()
	}
	closeErr := w.f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Abort closes the file and removes it.
func (w *Writer) Abort(_ context.Context) error {
	if w.committed || w.aborted {
		return nil
	}
	w.aborted = true
	_ = w.f.Close()
	return os.Remove(w.f.Name())
}

// BytesWritten returns the number of bytes written so far.
func (w *Writer) BytesWritten() int64 { return w.bytesWritten }

// Fd returns the underlying file descriptor.
func (w *Writer) Fd() uintptr {
	return w.f.Fd()
}

// IOSize returns the effective IO buffer size for this file.
func (w *Writer) IOSize() int {
	if w.cfg.IOSize > 0 {
		return w.cfg.IOSize
	}
	return model.ResolveIOSize(w.cfg.IOSizeTiers, w.size)
}
