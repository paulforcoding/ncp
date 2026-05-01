package model

// DiscoverItem is pushed to discoverCh by Walker and consumed by Replicators.
type DiscoverItem struct {
	SrcBase  string     // Source base directory, e.g. /data/
	DstBase  string     // Destination base, e.g. oss://bucket/backup/ or ncp://server:9900/path/
	RelPath  string     // Relative path from base, e.g. dir1/file.txt
	FileType FileType   // Regular, Dir, or Symlink
	FileSize int64      // Size in bytes (0 for Dir and Symlink)
	Mode     uint32     // File mode (permission bits)
	Uid      int        // Owner UID
	Gid      int        // Owner GID
	LinkTarget string   // Symlink target (only set when FileType == Symlink)
}
