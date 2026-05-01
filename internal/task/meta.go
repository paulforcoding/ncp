package task

import (
	"crypto/rand"
	"fmt"
	"time"
)

// GenerateTaskID creates a unique task ID: task-{timestamp}-{4hex}.
func GenerateTaskID() string {
	ts := time.Now().Format("20060102-150405")
	b := make([]byte, 2)
	rand.Read(b)
	return fmt.Sprintf("task-%s-%04x", ts, b)
}
