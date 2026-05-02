package progress

import (
	"github.com/zp001/ncp/pkg/model"
)

// ProgressStore persists per-file copy and checksum progress.
type ProgressStore interface {
	// Open creates or opens the store at the given directory path.
	Open(dir string) error

	// Get returns the copyStatus and cksumStatus for a relative path.
	// Returns CopyDiscovered, CksumNone if the key does not exist.
	Get(relPath string) (model.CopyStatus, model.CksumStatus, error)

	// Set writes the 2-byte status value for a relative path (NoSync).
	Set(relPath string, cs model.CopyStatus, cks model.CksumStatus) error

	// Batch returns a new write batch for bulk operations.
	Batch() Batch

	// Iter returns an iterator over all keys in order.
	// The caller must close the iterator.
	Iter() (Iterator, error)

	// Delete removes a key from the store.
	Delete(relPath string) error

	// Sync flushes pending writes to durable storage.
	Sync() error

	// Close releases all resources.
	Close() error

	// SetWalkComplete writes the __walk_complete marker with total count.
	SetWalkComplete(totalCount int64) error

	// HasWalkComplete returns true if __walk_complete marker exists.
	HasWalkComplete() (bool, error)

	// Reopen closes the DB and reopens it (for resume-without-walk_complete case).
	Reopen() error

	// Destroy closes and removes the DB directory.
	Destroy() error
}

// Batch accumulates writes and commits them atomically.
type Batch interface {
	Set(relPath string, cs model.CopyStatus, cks model.CksumStatus)
	Commit(sync bool) error
	Close()
}

// Iterator scans over progress store keys in order.
type Iterator interface {
	// First moves to the first key.
	First() bool
	// Next moves to the next key.
	Next() bool
	// Valid returns whether the iterator is positioned at a valid key.
	Valid() bool
	// Key returns the current relative path.
	Key() string
	// Value returns the current copyStatus and cksumStatus.
	Value() (model.CopyStatus, model.CksumStatus)
	// Close releases iterator resources.
	Close()
}
