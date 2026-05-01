package copy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/internal/progress/pebble"
	"github.com/zp001/ncp/pkg/model"
)

// Report is the structured completion report (FR33).
type Report struct {
	TaskID      string   `json:"taskId"`
	TotalFiles  int64    `json:"totalFiles"`
	DoneFiles   int64    `json:"doneFiles"`
	FailedFiles int64    `json:"failedFiles"`
	ExitCode    int      `json:"exitCode"`
	FailedList  []string `json:"failedFilesList,omitempty"`
}

// GenerateReport creates a completion report from DB and DBWriter stats.
func GenerateReport(taskID string, store *pebble.Store, done, failed int64, exitCode int) (*Report, error) {
	r := &Report{
		TaskID:      taskID,
		TotalFiles:  done + failed,
		DoneFiles:   done,
		FailedFiles: failed,
		ExitCode:    exitCode,
	}

	// Collect failed file paths from DB
	if failed > 0 {
		it, err := store.Iter()
		if err != nil {
			return nil, err
		}
		defer it.Close()

		for it.First(); it.Valid(); it.Next() {
			key := it.Key()
			if isInternalKey(key) {
				continue
			}
			cs, _ := it.Value()
			if cs == model.CopyError {
				r.FailedList = append(r.FailedList, key)
			}
		}
	}

	return r, nil
}

// WriteReport writes the report as JSON to a file and stdout.
func WriteReport(report *Report, reportPath string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}

	// Write to file
	if reportPath != "" {
		if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(reportPath, data, 0o644); err != nil {
			return err
		}
	}

	// Write to stdout (Agent-First: JSON output)
	fmt.Println(string(data))
	return nil
}
