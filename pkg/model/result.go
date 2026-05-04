package model

// FileResult is sent to resultCh by Replicators after processing a file.
type FileResult struct {
	RelPath     string
	FileType    FileType
	FileSize    int64
	CopyStatus  CopyStatus
	CksumStatus CksumStatus
	Checksum    string // copy 时计算的 checksum hex（仅 regular file）
	Algorithm   string // "md5" or "xxh64"
	SrcHash     string // cksum 时源端 hash（仅 cksum regular file）
	DstHash     string // cksum 时目的端 hash（仅 cksum regular file）
	Skipped     bool   // true if file was skipped by mtime/size/etag check
	Err         error  // 整体错误（保持兼容，用于 CopyStatus 和 resume 逻辑）
	MetadataErr error  // 元数据操作错误（Mkdir / Symlink / SetMetadata）
}
