package copy

import (
	"crypto/md5"
	"hash"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/zp001/ncp/pkg/model"
)

// NewHasher creates a hash.Hash for the given checksum algorithm.
func NewHasher(algo model.CksumAlgorithm) hash.Hash {
	switch algo {
	case model.CksumXXH64:
		return xxhash.New()
	default:
		return md5.New()
	}
}

// SumToHex returns the hex-encoded checksum string.
func SumToHex(h hash.Hash) string {
	return fmt.Sprintf("%x", h.Sum(nil))
}
