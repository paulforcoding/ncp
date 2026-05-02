package di

import (
	"fmt"

	"github.com/zp001/ncp/pkg/impls/progress/pebble"
	"github.com/zp001/ncp/pkg/interfaces/progress"
)

// NewProgressStore creates a new pebble-backed progress store.
func NewProgressStore(dbDir string) (progress.ProgressStore, error) {
	store := &pebble.Store{}
	if err := store.Open(dbDir); err != nil {
		return nil, fmt.Errorf("open progress store %s: %w", dbDir, err)
	}
	return store, nil
}
