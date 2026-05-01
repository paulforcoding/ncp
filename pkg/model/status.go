package model

// CopyStatus represents the copy progress status of a file (DB Value Byte 1).
type CopyStatus byte

const (
	CopyDiscovered CopyStatus = 1 + iota // Walker discovered, not pushed to channel
	CopyDispatched                        // Walker pushed to channel
	CopyDone                              // Copy completed
	CopyError                             // Copy failed
)

// CksumStatus represents the checksum verification status (DB Value Byte 2).
type CksumStatus byte

const (
	CksumNone     CksumStatus = 0 // Not verified
	CksumPending  CksumStatus = 1 // Pending verification
	CksumPass     CksumStatus = 2 // Checksum matched
	CksumMismatch CksumStatus = 3 // Checksum mismatch
	CksumError    CksumStatus = 4 // Checksum error
)

// FileType identifies the type of a discovered file.
type FileType byte

const (
	FileRegular FileType = 1 + iota
	FileDir
	FileSymlink
)

// EncodeDBValue packs copyStatus and cksumStatus into a 2-byte slice.
func EncodeDBValue(cs CopyStatus, cks CksumStatus) []byte {
	return []byte{byte(cs), byte(cks)}
}

// DecodeDBValue unpacks a 2-byte slice into copyStatus and cksumStatus.
func DecodeDBValue(val []byte) (CopyStatus, CksumStatus) {
	if len(val) == 0 {
		return CopyDiscovered, CksumNone
	}
	cs := CopyStatus(val[0])
	cks := CksumNone
	if len(val) > 1 {
		cks = CksumStatus(val[1])
	}
	return cs, cks
}
