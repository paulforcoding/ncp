package pebble

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"

	"github.com/zp001/ncp/internal/progress"
	"github.com/zp001/ncp/pkg/model"
)

const walkCompleteKey = "__walk_complete"

// Store implements progress.ProgressStore backed by Pebble.
type Store struct {
	db  *pebble.DB
	dir string
}

// Open creates or opens a Pebble DB at dir.
func (s *Store) Open(dir string) error {
	opts := &pebble.Options{
		DisableWAL: true, // NoSync mode: skip WAL for write performance
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return fmt.Errorf("pebble open %s: %w", dir, err)
	}
	s.db = db
	s.dir = dir
	return nil
}

// Get returns the copyStatus and cksumStatus for a relative path.
func (s *Store) Get(relPath string) (model.CopyStatus, model.CksumStatus, error) {
	val, closer, err := s.db.Get([]byte(relPath))
	if err != nil {
		if err == pebble.ErrNotFound {
			return model.CopyDiscovered, model.CksumNone, nil
		}
		return model.CopyDiscovered, model.CksumNone, fmt.Errorf("pebble get %s: %w", relPath, err)
	}
	defer closer.Close()
	cs, cks := decodeValue(val)
	return cs, cks, nil
}

// Set writes the 2-byte status value for a relative path (NoSync).
func (s *Store) Set(relPath string, cs model.CopyStatus, cks model.CksumStatus) error {
	if err := s.db.Set([]byte(relPath), encodeValue(cs, cks), pebble.NoSync); err != nil {
		return fmt.Errorf("pebble set %s: %w", relPath, err)
	}
	return nil
}

// Batch returns a new Pebble write batch.
func (s *Store) Batch() progress.Batch {
	return &batch{b: s.db.NewBatch(), db: s.db}
}

// Iter returns an iterator over all keys in order.
func (s *Store) Iter() (progress.Iterator, error) {
	iter, _ := s.db.NewIter(nil)
	iter.First()
	return &iterator{iter: iter}, nil
}

// Delete removes a key from the store.
func (s *Store) Delete(relPath string) error {
	if err := s.db.Delete([]byte(relPath), pebble.NoSync); err != nil {
		return fmt.Errorf("pebble delete %s: %w", relPath, err)
	}
	return nil
}

// Sync flushes pending writes to durable storage.
func (s *Store) Sync() error {
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}
	return nil
}

// Close releases all resources.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// SetWalkComplete writes the __walk_complete marker with total count.
// Since the DB runs with DisableWAL=true, we flush memtables to ensure
// durability instead of using pebble.Sync (which requires a WAL).
func (s *Store) SetWalkComplete(totalCount int64) error {
	key := []byte(walkCompleteKey)
	val := []byte(fmt.Sprintf("%d", totalCount))
	if err := s.db.Set(key, val, pebble.NoSync); err != nil {
		return fmt.Errorf("pebble set walk_complete: %w", err)
	}
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("pebble flush walk_complete: %w", err)
	}
	return nil
}

// HasWalkComplete returns true if __walk_complete marker exists.
func (s *Store) HasWalkComplete() (bool, error) {
	_, closer, err := s.db.Get([]byte(walkCompleteKey))
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, fmt.Errorf("pebble get walk_complete: %w", err)
	}
	closer.Close()
	return true, nil
}

// Reopen closes the DB and reopens it (for resume-without-walk_complete case).
func (s *Store) Reopen() error {
	if err := s.Close(); err != nil {
		return fmt.Errorf("pebble close for reopen: %w", err)
	}
	return s.Open(s.dir)
}

// Destroy closes and removes the Pebble DB directory.
func (s *Store) Destroy() error {
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("pebble close before destroy: %w", err)
	}
	s.db = nil
	if err := os.RemoveAll(s.dir); err != nil {
		return fmt.Errorf("remove pebble dir %s: %w", s.dir, err)
	}
	return nil
}

// batch wraps a pebble.Batch.
type batch struct {
	b  *pebble.Batch
	db *pebble.DB
}

func (ba *batch) Set(relPath string, cs model.CopyStatus, cks model.CksumStatus) {
	_ = ba.b.Set([]byte(relPath), encodeValue(cs, cks), nil)
}

func (ba *batch) Commit(sync bool) error {
	var opts *pebble.WriteOptions
	if sync {
		opts = pebble.NoSync // DisableWAL=true: Sync would fail, use NoSync+Flush instead
	} else {
		opts = pebble.NoSync
	}
	if err := ba.b.Commit(opts); err != nil {
		return fmt.Errorf("pebble batch commit: %w", err)
	}
	if sync {
		if err := ba.db.Flush(); err != nil {
			return fmt.Errorf("pebble batch flush: %w", err)
		}
	}
	return nil
}

func (ba *batch) Close() {
	ba.b.Close()
}

// iterator wraps a pebble.Iterator.
type iterator struct {
	iter *pebble.Iterator
}

func (it *iterator) First() bool {
	return it.iter.First()
}

func (it *iterator) Next() bool {
	return it.iter.Next()
}

func (it *iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *iterator) Key() string {
	return string(it.iter.Key())
}

func (it *iterator) Value() (model.CopyStatus, model.CksumStatus) {
	return decodeValue(it.iter.Value())
}

func (it *iterator) Close() {
	it.iter.Close()
}
