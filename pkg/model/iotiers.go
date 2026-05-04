package model

// IOSizeTier defines an IO size for a file size range.
type IOSizeTier struct {
	MinFileSize int64 // Minimum file size (inclusive), 0 means no lower bound
	MaxFileSize int64 // Maximum file size (exclusive), 0 means no upper bound
	IOSize      int   // IO size in bytes for this range
}

// DefaultIOSizeTiers returns the default tiered IO size configuration
// aligned with acp defaults.
func DefaultIOSizeTiers() []IOSizeTier {
	const (
		KB     = 1024
		MB     = 1024 * KB
		_1MB   = 1 * MB
		_100MB = 100 * MB
		_128KB = 128 * KB
		_1MBIO = 1 * MB
		_4MBIO = 4 * MB
	)
	return []IOSizeTier{
		{MinFileSize: 0, MaxFileSize: _1MB, IOSize: _128KB},
		{MinFileSize: _1MB, MaxFileSize: _100MB, IOSize: _1MBIO},
		{MinFileSize: _100MB, MaxFileSize: 0, IOSize: _4MBIO},
	}
}

// ResolveIOSize returns the IO size for a given file size based on tier config.
// If no tier matches, returns the last tier's IO size.
func ResolveIOSize(tiers []IOSizeTier, fileSize int64) int {
	for _, t := range tiers {
		if fileSize < t.MinFileSize {
			continue
		}
		if t.MaxFileSize > 0 && fileSize >= t.MaxFileSize {
			continue
		}
		return t.IOSize
	}
	if len(tiers) > 0 {
		return tiers[len(tiers)-1].IOSize
	}
	return 128 * 1024
}
