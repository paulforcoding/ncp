package cksum

import (
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/model"
)

// CksumReport is the structured checksum verification report.
type CksumReport struct {
	TaskID        string   `json:"taskId"`
	TotalFiles    int64    `json:"totalFiles"`
	PassFiles     int64    `json:"passFiles"`
	MismatchFiles int64    `json:"mismatchFiles"`
	ErrorFiles    int64    `json:"errorFiles"`
	ExitCode      int      `json:"exitCode"`
	MismatchList  []string `json:"mismatchFilesList,omitempty"`
	ErrorList     []string `json:"errorFilesList,omitempty"`
}

// GenerateCksumReport creates a checksum report from DB and CksumDBWriter stats.
func GenerateCksumReport(taskID string, store progress.ProgressStore, pass, mismatch, failed int64, exitCode int) (*CksumReport, error) {
	r := &CksumReport{
		TaskID:        taskID,
		TotalFiles:    pass + mismatch + failed,
		PassFiles:     pass,
		MismatchFiles: mismatch,
		ErrorFiles:    failed,
		ExitCode:      exitCode,
	}

	if mismatch > 0 || failed > 0 {
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
			_, cks := it.Value()
			switch cks {
			case model.CksumMismatch:
				r.MismatchList = append(r.MismatchList, key)
			case model.CksumError:
				r.ErrorList = append(r.ErrorList, key)
			}
		}
	}

	return r, nil
}

func isInternalKey(key string) bool {
	return len(key) >= 2 && key[0] == '_' && key[1] == '_'
}
