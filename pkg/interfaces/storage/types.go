package storage

import (
	"errors"
	"os"
	"time"

	"github.com/zp001/ncp/pkg/model"
)

// FileAttr collects POSIX-level metadata for a file or directory.
// Adding fields does not break the interface.
type FileAttr struct {
	Mode          os.FileMode
	Uid, Gid      int
	Mtime         time.Time
	Atime         time.Time
	Xattr         map[string]string
	SymlinkTarget string // valid only when FileType == FileSymlink
}

// DiscoverItem is the work unit produced by Walker and consumed by Replicator.
type DiscoverItem struct {
	RelPath   string
	FileType  model.FileType
	Size      int64    // valid only when FileType == FileRegular
	Attr      FileAttr // POSIX metadata
	Checksum  []byte   // pre-computed source checksum (e.g. OSS ETag bytes); nil = unknown
	Algorithm string   // "md5" / "etag-md5" / "etag-multipart" / ""
}

// Sentinel errors. Backends must wrap their underlying SDK errors with these
// (e.g. fmt.Errorf("aliyun head %s: %w", relPath, ErrNotFound)) so callers can
// inspect with errors.Is.
var (
	ErrNotFound        = errors.New("storage: not found")
	ErrAlreadyExists   = errors.New("storage: already exists")
	ErrPermission      = errors.New("storage: permission denied")
	ErrInvalidArgument = errors.New("storage: invalid argument")
	ErrChecksum        = errors.New("storage: checksum mismatch")
	ErrUnsupported     = errors.New("storage: operation not supported")
)

// CksumChunkSize is the fixed chunk size for checksum computation (1 MiB).
const CksumChunkSize = 1 << 20

// HashResult holds the output of a checksum computation.
// WholeFileHash is the cumulative hash at EOF = MD5(file_content), hex-encoded.
// ChunkHashes are per-CksumChunkSize chunk hashes, hex-encoded.
type HashResult struct {
	WholeFileHash string
	ChunkHashes   []string
	Algo          string
	Err           error
}
