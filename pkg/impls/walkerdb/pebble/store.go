package pebble

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/walkerdb"
)

// Store implements walkerdb.Store using PebbleDB.
type Store struct {
	db  *pebble.DB
	dir string
}

var _ walkerdb.Store = (*Store)(nil)

var (
	walkCompleteKey = []byte("__walk_complete__")
)

func seqKey(seq int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq))
	return b[:]
}

// Open opens or creates a PebbleDB at dir.
func (s *Store) Open(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir walkerdb: %w", err)
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("open pebble walkerdb: %w", err)
	}
	s.db = db
	s.dir = dir
	return nil
}

// Close closes the underlying PebbleDB.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Destroy closes and removes the database directory.
func (s *Store) Destroy() error {
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	s.db = nil
	return os.RemoveAll(s.dir)
}

// Put writes a ListEntry at the given sequence number.
func (s *Store) Put(seq int64, entry protocol.ListEntry) error {
	key := seqKey(seq)
	val := entry.Encode()
	return s.db.Set(key, val, pebble.Sync)
}

// GetRange reads entries starting from startSeq, up to limit entries.
func (s *Store) GetRange(startSeq int64, limit int) ([]walkerdb.Entry, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: seqKey(startSeq),
		UpperBound: seqKey(startSeq + int64(limit) + 1),
	})
	if err != nil {
		return nil, fmt.Errorf("new iter: %w", err)
	}
	defer iter.Close()

	var entries []walkerdb.Entry
	for iter.First(); iter.Valid() && len(entries) < limit; iter.Next() {
		key := iter.Key()
		if len(key) != 8 {
			continue
		}
		seq := int64(binary.BigEndian.Uint64(key))
		var entry protocol.ListEntry
		if err := entry.Decode(iter.Value()); err != nil {
			return nil, fmt.Errorf("decode entry at seq %d: %w", seq, err)
		}
		entries = append(entries, walkerdb.Entry{Seq: seq, Entry: entry})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

// SetWalkComplete marks the walk as finished.
func (s *Store) SetWalkComplete() error {
	return s.db.Set(walkCompleteKey, []byte{1}, pebble.Sync)
}

// IsWalkComplete returns true if the walk has finished.
func (s *Store) IsWalkComplete() (bool, error) {
	_, closer, err := s.db.Get(walkCompleteKey)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}
