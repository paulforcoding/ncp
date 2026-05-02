package copy

import (
	"sync"
	"time"
)

const throughputWindow = 10 * time.Second

type throughputSample struct {
	files int64
	bytes int64
	at    time.Time
}

// ThroughputMeter tracks copy throughput using a sliding window.
type ThroughputMeter struct {
	mu      sync.Mutex
	files   int64
	bytes   int64
	samples []throughputSample
}

// AddFile records a completed file.
func (m *ThroughputMeter) AddFile(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files++
	m.bytes += size
	m.samples = append(m.samples, throughputSample{files: 1, bytes: size, at: time.Now()})
}

// Rate returns the current throughput rates (files/s, bytes/s) over the sliding window.
func (m *ThroughputMeter) Rate() (filesPerSec, bytesPerSec float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-throughputWindow)

	// Trim old samples
	i := 0
	for i < len(m.samples) && m.samples[i].at.Before(cutoff) {
		i++
	}
	m.samples = m.samples[i:]

	if len(m.samples) == 0 {
		return 0, 0
	}

	var sf, sb int64
	for _, s := range m.samples {
		sf += s.files
		sb += s.bytes
	}

	elapsed := now.Sub(m.samples[0].at).Seconds()
	if elapsed < 0.5 {
		elapsed = 0.5
	}

	return float64(sf) / elapsed, float64(sb) / elapsed
}

// Totals returns the cumulative file count and byte count.
func (m *ThroughputMeter) Totals() (files, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.files, m.bytes
}
