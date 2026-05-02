package di

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// MultiSource wraps multiple storage.Source instances into a single Source.
// Each source's items are prefixed with its directory name under a common root,
// mimicking `cp src1 src2 dst/` behavior where dst gets src1/..., src2/...
type MultiSource struct {
	entries []multiSourceEntry
	base    string
}

type multiSourceEntry struct {
	prefix string           // e.g. "project-a/" — trailing slash for dirs
	src    storage.Source
}

// NewMultiSource creates a Source that concatenates multiple sources.
// Each source's items appear under its base directory name in the destination.
// e.g. sources=[/data/a, /data/b] → items have RelPath "a/...", "b/..."
func NewMultiSource(sources []storage.Source) (*MultiSource, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("multi-source: at least one source required")
	}

	entries := make([]multiSourceEntry, len(sources))
	for i, src := range sources {
		name := filepath.Base(src.Base())
		entries[i] = multiSourceEntry{
			prefix: name + "/",
			src:    src,
		}
	}

	return &MultiSource{entries: entries, base: ""}, nil
}

// Walk walks each source sequentially, prefixing RelPath with the source's name.
func (ms *MultiSource) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
	for _, e := range ms.entries {
		if err := e.src.Walk(ctx, func(item model.DiscoverItem) error {
			item.RelPath = e.prefix + item.RelPath
			item.SrcBase = ms.Base()
			return fn(item)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Open routes to the correct source by matching the RelPath prefix.
func (ms *MultiSource) Open(relPath string) (storage.Reader, error) {
	e, innerPath := ms.route(relPath)
	if e == nil {
		return nil, fmt.Errorf("multi-source: no source for path %q", relPath)
	}
	return e.src.Open(innerPath)
}

// Restat routes to the correct source by matching the RelPath prefix.
func (ms *MultiSource) Restat(relPath string) (model.DiscoverItem, error) {
	e, innerPath := ms.route(relPath)
	if e == nil {
		return model.DiscoverItem{}, fmt.Errorf("multi-source: no source for path %q", relPath)
	}
	item, err := e.src.Restat(innerPath)
	if err != nil {
		return item, err
	}
	item.RelPath = relPath
	item.SrcBase = ms.Base()
	return item, nil
}

// Base returns a synthetic base path for the multi-source.
func (ms *MultiSource) Base() string {
	if ms.base != "" {
		return ms.base
	}
	// Use the common parent directory of all sources
	if len(ms.entries) > 0 {
		return filepath.Dir(ms.entries[0].src.Base())
	}
	return ""
}

func (ms *MultiSource) route(relPath string) (*multiSourceEntry, string) {
	for i := range ms.entries {
		if strings.HasPrefix(relPath, ms.entries[i].prefix) {
			return &ms.entries[i], strings.TrimPrefix(relPath, ms.entries[i].prefix)
		}
	}
	return nil, relPath
}
