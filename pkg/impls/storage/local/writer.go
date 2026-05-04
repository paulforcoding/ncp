package local

import (
	"context"
	"os"

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

// Writer wraps os.File and provides WriteAt (pwrite) semantics
// with configurable DirectIO, SyncWrites, and IOSizeTiers.
type Writer struct {
	f    *os.File
	cfg  WriterConfig
	size int64 // expected file size for IOSize tier resolution
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
func (w *Writer) WriteAt(p []byte, off int64) (int, error) {
	return w.f.WriteAt(p, off)
}

// Sync calls fsync on the underlying file if SyncWrites is enabled.
func (w *Writer) Sync() error {
	if w.cfg.SyncWrites {
		return w.f.Sync()
	}
	return nil
}

// Close closes the file. SyncWrites triggers a final fsync before close.
// The checksum parameter is ignored for local copies.
func (w *Writer) Close(_ context.Context, _ []byte) error {
	if w.cfg.SyncWrites {
		w.f.Sync()
	}
	return w.f.Close()
}

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
