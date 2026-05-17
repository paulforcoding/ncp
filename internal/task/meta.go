package task

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type JobType string

const (
	JobTypeCopy  JobType = "copy"
	JobTypeCksum JobType = "cksum"
)

type RunStatus string

const (
	RunStatusRunning RunStatus = "running"
	RunStatusDone    RunStatus = "done"
	RunStatusFailed  RunStatus = "failed"
)

type RunRecord struct {
	ID         string     `json:"id"`
	JobType    JobType    `json:"jobType"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Status     RunStatus  `json:"status"`
	ExitCode   int        `json:"exitCode,omitempty"`
}

type Meta struct {
	TaskID         string      `json:"taskId"`
	SrcBase        string      `json:"srcBase"`
	DstBase        string      `json:"dstBase"`
	BasenamePrefix bool        `json:"basenamePrefix"`
	CreatedAt      time.Time   `json:"createdAt"`
	CmdArgs        []string    `json:"cmdArgs"`
	PID            int         `json:"pid"`
	Hostname       string      `json:"hostname"`
	Runs           []RunRecord `json:"runs"`
}

// GenerateTaskID creates a unique task ID: task-{timestamp}-{4hex}.
func GenerateTaskID() string {
	ts := time.Now().Format("20060102-150405")
	b := make([]byte, 2)
	_, _ = rand.Read(b)
	return fmt.Sprintf("task-%s-%04x", ts, b)
}

// GenerateRunID creates a unique run ID: run-{timestamp}-{4hex}.
func GenerateRunID() string {
	ts := time.Now().Format("20060102-150405")
	b := make([]byte, 2)
	_, _ = rand.Read(b)
	return fmt.Sprintf("run-%s-%04x", ts, b)
}

// NewMeta creates a Meta with current PID/hostname and an initial RunRecord.
func NewMeta(taskID, srcBase, dstBase string, cmdArgs []string, jobType JobType) *Meta {
	hostname, _ := os.Hostname()
	now := time.Now()
	return &Meta{
		TaskID:    taskID,
		SrcBase:   srcBase,
		DstBase:   dstBase,
		CreatedAt: now,
		CmdArgs:   cmdArgs,
		PID:       os.Getpid(),
		Hostname:  hostname,
		Runs: []RunRecord{
			{
				ID:        GenerateRunID(),
				JobType:   jobType,
				StartedAt: now,
				Status:    RunStatusRunning,
			},
		},
	}
}

// MetaPath returns the path to meta.json for a task.
func MetaPath(progressDir, taskID string) string {
	return filepath.Join(progressDir, taskID, "meta.json")
}

// TaskDir returns the task directory path.
func TaskDir(progressDir, taskID string) string {
	return filepath.Join(progressDir, taskID)
}

// WriteMetaTo writes meta.json to progressDir/{taskID}/meta.json.
func WriteMetaTo(meta *Meta, progressDir string) error {
	path := MetaPath(progressDir, meta.TaskID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir meta: %w", err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}
	return nil
}

// ReadMeta reads and unmarshals meta.json for a task.
func ReadMeta(progressDir, taskID string) (*Meta, error) {
	path := MetaPath(progressDir, taskID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read meta %s: %w", taskID, err)
	}
	var meta Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta %s: %w", taskID, err)
	}
	return &meta, nil
}

// UpdateRunFinished sets the last run's FinishedAt, Status, and ExitCode, then writes back.
func UpdateRunFinished(meta *Meta, exitCode int, progressDir string) error {
	if len(meta.Runs) == 0 {
		return fmt.Errorf("no runs in meta for task %s", meta.TaskID)
	}
	now := time.Now()
	run := &meta.Runs[len(meta.Runs)-1]
	run.FinishedAt = &now
	if exitCode == 0 {
		run.Status = RunStatusDone
	} else {
		run.Status = RunStatusFailed
	}
	run.ExitCode = exitCode
	return WriteMetaTo(meta, progressDir)
}

// AppendRun adds a new RunRecord to the meta and writes back.
func AppendRun(meta *Meta, jobType JobType, progressDir string) error {
	meta.Runs = append(meta.Runs, RunRecord{
		ID:        GenerateRunID(),
		JobType:   jobType,
		StartedAt: time.Now(),
		Status:    RunStatusRunning,
	})
	meta.PID = os.Getpid()
	if hostname, err := os.Hostname(); err == nil {
		meta.Hostname = hostname
	}
	return WriteMetaTo(meta, progressDir)
}

// LastJobType returns the job type of the most recent run.
func LastJobType(meta *Meta) JobType {
	if len(meta.Runs) == 0 {
		return JobTypeCopy
	}
	return meta.Runs[len(meta.Runs)-1].JobType
}

// CheckTaskNotRunning verifies a task is not running by checking PID and acquiring flock.
// Returns the Meta and TaskLock if safe to proceed, or an error if the task is running.
func CheckTaskNotRunning(progressDir, taskID string) (*Meta, *TaskLock, error) {
	meta, err := ReadMeta(progressDir, taskID)
	if err != nil {
		return nil, nil, fmt.Errorf("read meta: %w", err)
	}

	// Fast check: PID alive?
	if IsProcessAlive(meta.PID) {
		return nil, nil, fmt.Errorf("task %s is running (pid %d)", taskID, meta.PID)
	}

	// Hard guarantee: flock
	dir := TaskDir(progressDir, taskID)
	lock, err := AcquireTaskLock(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("task %s is locked: %w", taskID, err)
	}
	return meta, lock, nil
}
