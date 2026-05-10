package cksum

import (
	"context"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// ShouldSkipCksum checks if a file can be skipped during checksum verification.
// Same logic as copy skip: mtime+size+etag match means files are identical.
func ShouldSkipCksum(ctx context.Context, dst storage.Source, item storage.DiscoverItem) (bool, error) {
	dstItem, err := dst.Stat(ctx, item.RelPath)
	if err != nil {
		return false, nil
	}
	return copy.MatchSkip(item, dstItem), nil
}
