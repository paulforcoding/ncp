package cksum

import (
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// ShouldSkipCksum checks if a file can be skipped during checksum verification.
// Same logic as copy skip: mtime+size+etag match means files are identical.
func ShouldSkipCksum(dst storage.Source, item model.DiscoverItem) (bool, error) {
	dstItem, err := dst.Restat(item.RelPath)
	if err != nil {
		return false, nil
	}
	return copy.MatchSkip(item, dstItem), nil
}
