package di

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// BasenamePrefixedSource wraps one or more storage.Source instances,
// prefixing each item's RelPath with its source's basename.
// This unifies single-source and multi-source copy behavior:
// ncp copy /data/dir /tmp/  →  items have RelPath "dir/..."
type BasenamePrefixedSource struct {
	entries []basenamePrefixedSourceEntry
	base    string
}

type basenamePrefixedSourceEntry struct {
	prefix string // e.g. "project-a/" — trailing slash for dirs, stripped for single files
	src    storage.Source
}

// NewBasenamePrefixedSource creates a Source that prefixes each source's items
// with its base directory/file name.
// e.g. sources=[/data/a, /data/b] → items have RelPath "a/...", "b/..."
func NewBasenamePrefixedSource(sources []storage.Source, basenames []string) (*BasenamePrefixedSource, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("basename-prefixed source: at least one source required")
	}
	if len(sources) != len(basenames) {
		return nil, fmt.Errorf("basename-prefixed source: source count (%d) != basename count (%d)", len(sources), len(basenames))
	}

	entries := make([]basenamePrefixedSourceEntry, len(sources))
	for i, src := range sources {
		entries[i] = basenamePrefixedSourceEntry{
			prefix: basenames[i] + "/",
			src:    src,
		}
	}

	return &BasenamePrefixedSource{entries: entries, base: ""}, nil
}

// Walk walks each source sequentially, prefixing RelPath with the source's basename.
func (ms *BasenamePrefixedSource) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
	for _, e := range ms.entries {
		if err := e.src.Walk(ctx, func(item model.DiscoverItem) error {
			if item.RelPath == "" {
				// Single-file source: emit basename (no trailing slash)
				item.RelPath = strings.TrimSuffix(e.prefix, "/")
			} else {
				item.RelPath = e.prefix + item.RelPath
			}
			item.SrcBase = ms.Base()
			return fn(item)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Open routes to the correct source by matching the RelPath prefix.
func (ms *BasenamePrefixedSource) Open(relPath string) (storage.Reader, error) {
	e, innerPath := ms.route(relPath)
	if e == nil {
		return nil, fmt.Errorf("basename-prefixed source: no source for path %q", relPath)
	}
	return e.src.Open(innerPath)
}

// Restat routes to the correct source by matching the RelPath prefix.
func (ms *BasenamePrefixedSource) Restat(relPath string) (model.DiscoverItem, error) {
	e, innerPath := ms.route(relPath)
	if e == nil {
		return model.DiscoverItem{}, fmt.Errorf("basename-prefixed source: no source for path %q", relPath)
	}
	item, err := e.src.Restat(innerPath)
	if err != nil {
		return item, err
	}
	item.RelPath = relPath
	item.SrcBase = ms.Base()
	return item, nil
}

// Base returns a synthetic base path for the source.
func (ms *BasenamePrefixedSource) Base() string {
	if ms.base != "" {
		return ms.base
	}
	if len(ms.entries) > 0 {
		return filepath.Dir(ms.entries[0].src.Base())
	}
	return ""
}

func (ms *BasenamePrefixedSource) route(relPath string) (*basenamePrefixedSourceEntry, string) {
	for i := range ms.entries {
		prefix := ms.entries[i].prefix
		if strings.HasPrefix(relPath, prefix) {
			return &ms.entries[i], strings.TrimPrefix(relPath, prefix)
		}
		// Single-file source: exact basename match (prefix without trailing slash)
		barePrefix := strings.TrimSuffix(prefix, "/")
		if relPath == barePrefix {
			return &ms.entries[i], ""
		}
	}
	return nil, relPath
}

// SourceBasename derives a basename for a source, used by BasenamePrefixedSource.
// It handles special cases like OSS whole-bucket copy.
func SourceBasename(src storage.Source, originalURL string) string {
	u, _ := ParsePath(originalURL)
	switch u.Scheme {
	case "oss":
		bucket, prefix := parseOSSURL(u)
		if prefix == "" {
			return bucket
		}
		prefix = strings.TrimSuffix(prefix, "/")
		return filepath.Base(prefix)
	case "ncp":
		return filepath.Base(u.Path)
	default:
		return filepath.Base(u.Path)
	}
}
