package copy

import (
	"context"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// ShouldSkipCopy checks if a file can be skipped during copy.
// Returns true if dst already has an identical file (same mtime+size for local,
// or matching ETag for OSS single-part uploads).
func ShouldSkipCopy(ctx context.Context, dst storage.Destination, item model.DiscoverItem) (bool, error) {
	restatter, ok := dst.(storage.Restatter)
	if !ok {
		return false, nil
	}

	dstItem, err := restatter.Restat(ctx, item.RelPath)
	if err != nil {
		return false, nil
	}

	return MatchSkip(item, dstItem), nil
}

// MatchSkip determines if src and dst items are identical for skip purposes.
func MatchSkip(src, dst model.DiscoverItem) bool {
	if src.FileType != dst.FileType {
		return false
	}

	switch src.FileType {
	case model.FileDir:
		return true
	case model.FileSymlink:
		return src.LinkTarget == dst.LinkTarget
	case model.FileRegular:
		if src.FileSize != dst.FileSize {
			return false
		}
		if src.ETag != "" && dst.ETag != "" {
			return src.ETag == dst.ETag
		}
		if src.Mtime != 0 && src.Mtime == dst.Mtime {
			return true
		}
		return false
	default:
		return false
	}
}
