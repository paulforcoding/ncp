package filelog

// FileLog event types.
const (
	EventCopyPlan        = "copy_plan"
	EventFileStart       = "file_start" // reserved, not used yet
	EventFileComplete    = "file_complete"
	EventProgressSummary = "progress_summary"
)
