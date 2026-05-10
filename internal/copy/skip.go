package copy

import (
	"bytes"
	"context"
	"errors"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// ShouldSkipCopy checks if a file can be skipped during copy.
// Returns true if dst already has an identical file (same mtime+size for local,
// or matching ETag for OSS single-part uploads).
func ShouldSkipCopy(ctx context.Context, dst storage.Destination, item storage.DiscoverItem) (bool, error) {
	dstItem, err := dst.Stat(ctx, item.RelPath)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, nil
	}

	return MatchSkip(item, dstItem), nil
}

// MatchSkip determines if src and dst items are identical for skip purposes.
func MatchSkip(src, dst storage.DiscoverItem) bool {
	if src.FileType != dst.FileType {
		return false
	}

	switch src.FileType {
	case model.FileDir:
		return true
	case model.FileSymlink:
		return src.Attr.SymlinkTarget == dst.Attr.SymlinkTarget
	case model.FileRegular:
		if src.Size != dst.Size {
			return false
		}
		if len(src.Checksum) > 0 && len(dst.Checksum) > 0 {
			return bytes.Equal(src.Checksum, dst.Checksum) && src.Algorithm == dst.Algorithm
		}
		if !src.Attr.Mtime.IsZero() && src.Attr.Mtime.Equal(dst.Attr.Mtime) {
			return true
		}
		return false
	default:
		return false
	}
}
