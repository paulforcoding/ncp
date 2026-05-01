package model

// FileResult is sent to resultCh by Replicators after processing a file.
type FileResult struct {
	RelPath    string     // Relative path from base
	CopyStatus CopyStatus // Final copy status (done or error)
	CksumStatus CksumStatus // Checksum status (populated during cksum operations)
	Err        error     // Non-nil if copy failed
}
