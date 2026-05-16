package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/task"
)

// runTaskList handles `ncp task list`.
func runTaskList(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")

	entries, err := os.ReadDir(progressDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read progress dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := task.ReadMeta(progressDir, entry.Name())
		if err != nil {
			continue
		}
		line, _ := json.Marshal(meta)
		fmt.Println(string(line))
	}
	return nil
}

// runTaskShow handles `ncp task show <taskID>`.
func runTaskShow(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")
	taskID := args[0]

	meta, err := task.ReadMeta(progressDir, taskID)
	if err != nil {
		return fmt.Errorf("task %s not found: %w", taskID, err)
	}

	out, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println(string(out))
	return nil
}

// runTaskDelete handles `ncp task delete <taskID>`.
func runTaskDelete(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")
	taskID := args[0]

	// Check if task is running
	_, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return fmt.Errorf("cannot delete: %w", err)
	}
	if lock != nil {
		_ = lock.Release()
	}

	dir := task.TaskDir(progressDir, taskID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete task %s: %w", taskID, err)
	}

	fmt.Printf("{\"taskId\":%q,\"action\":\"deleted\"}\n", taskID)
	return nil
}
