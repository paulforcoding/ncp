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
	Err         error
}
