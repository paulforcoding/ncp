package model

import "fmt"

// CksumAlgorithm defines the checksum algorithm for data verification.
type CksumAlgorithm string

const (
	CksumMD5   CksumAlgorithm = "md5"
	CksumXXH64 CksumAlgorithm = "xxh64"
)

// DefaultCksumAlgorithm is the default checksum algorithm.
const DefaultCksumAlgorithm = CksumMD5

// ParseCksumAlgorithm parses a string into a CksumAlgorithm, returning an error for unknown values.
func ParseCksumAlgorithm(s string) (CksumAlgorithm, error) {
	switch CksumAlgorithm(s) {
	case CksumMD5:
		return CksumMD5, nil
	case CksumXXH64:
		return CksumXXH64, nil
	default:
		return "", fmt.Errorf("unknown checksum algorithm: %q (supported: md5, xxh64)", s)
	}
}
