package filelog

// FileLog event types (FR32, FR34, FR35).
const (
	EventCopyPlan       = "copy_plan"
	EventWalkProgress   = "walk_progress"
	EventFileStart      = "file_start"
	EventFileComplete   = "file_complete"
	EventFileError      = "file_error"
	EventProgressSummary = "progress_summary"
	EventCopyComplete   = "copy_complete"
)

// ProgressSummaryData is the payload for progress_summary events.
type ProgressSummaryData struct {
	TotalFiles   int64   `json:"totalFiles"`
	DoneFiles    int64   `json:"doneFiles"`
	FailedFiles  int64   `json:"failedFiles"`
	SpeedMBps    float64 `json:"speedMBps"`
	ETASeconds   int64   `json:"etaSeconds"`
}

// FileCompleteData is the payload for file_complete events.
type FileCompleteData struct {
	RelPath  string `json:"relPath"`
	FileSize int64  `json:"fileSize"`
}

// FileErrorData is the payload for file_error events.
type FileErrorData struct {
	RelPath string `json:"relPath"`
	Error   string `json:"error"`
}

// WalkProgressData is the payload for walk_progress events.
type WalkProgressData struct {
	DiscoveredCount int    `json:"discoveredCount"`
	CurrentPath     string `json:"currentPath"`
}
