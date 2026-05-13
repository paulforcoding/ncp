package walkerdb

import (
	"github.com/zp001/ncp/internal/protocol"
)

// Entry is a single walk result with its sequence number.
type Entry struct {
	Seq   int64
	Entry protocol.ListEntry
}

// Store persists directory walk results for paginated retrieval.
type Store interface {
	Open(dir string) error
	Close() error
	Destroy() error

	// Put writes an entry at the given sequence number.
	Put(seq int64, entry protocol.ListEntry) error

	// GetRange reads entries starting from startSeq, up to limit entries.
	GetRange(startSeq int64, limit int) ([]Entry, error)

	// SetWalkComplete marks the walk as finished.
	SetWalkComplete() error

	// IsWalkComplete returns true if the walk has finished.
	IsWalkComplete() (bool, error)
}
