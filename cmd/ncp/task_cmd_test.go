package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zp001/ncp/internal/task"
)

// newTaskCmd creates a cobra.Command with the ProgressStorePath flag set to dir.
// Used by all task-command tests to drive runTaskList/Show/Delete in-process.
func newTaskCmd(t *testing.T, dir string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "task"}
	cmd.Flags().String("ProgressStorePath", dir, "")
	return cmd
}

// captureStdout runs fn and returns whatever it wrote to os.Stdout.
// runTaskList/Show/Delete write directly to os.Stdout (CLI protocol output),
// so to assert on the output we have to redirect the real fd.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := os.Stdout
	os.Stdout = w

	done := make(chan struct{})
	var buf bytes.Buffer
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	runErr := fn()
	_ = w.Close()
	<-done
	os.Stdout = orig
	_ = r.Close()
	return buf.String(), runErr
}

func TestRunTaskList_DirNotExist(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "absent")
	cmd := newTaskCmd(t, tmp)

	out, err := captureStdout(t, func() error {
		return runTaskList(cmd, nil)
	})
	if err != nil {
		t.Fatalf("runTaskList returned error for missing dir: %v", err)
	}
	if out != "" {
		t.Errorf("expected no output for missing dir, got %q", out)
	}
}

func TestRunTaskList_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	cmd := newTaskCmd(t, dir)

	out, err := captureStdout(t, func() error {
		return runTaskList(cmd, nil)
	})
	if err != nil {
		t.Fatalf("runTaskList: %v", err)
	}
	if out != "" {
		t.Errorf("expected no output for empty dir, got %q", out)
	}
}

func TestRunTaskList_OneTask(t *testing.T) {
	dir := t.TempDir()
	taskID := "task-test-aaaa"
	meta := task.NewMeta(taskID, "/src", "/dst", []string{"copy", "/src", "/dst"}, task.JobTypeCopy)
	if err := task.WriteMetaTo(meta, dir); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	cmd := newTaskCmd(t, dir)
	out, err := captureStdout(t, func() error {
		return runTaskList(cmd, nil)
	})
	if err != nil {
		t.Fatalf("runTaskList: %v", err)
	}

	line := strings.TrimSpace(out)
	if line == "" {
		t.Fatal("expected one JSON line, got empty output")
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("unmarshal: %v\nline: %s", err, line)
	}
	if got["taskId"] != taskID {
		t.Errorf("taskId = %v, want %s", got["taskId"], taskID)
	}
}

func TestRunTaskShow_NotFound(t *testing.T) {
	dir := t.TempDir()
	cmd := newTaskCmd(t, dir)

	err := runTaskShow(cmd, []string{"task-nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent task, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestRunTaskShow_ValidMeta(t *testing.T) {
	dir := t.TempDir()
	taskID := "task-show-bbbb"
	meta := task.NewMeta(taskID, "/src", "/dst", []string{"cksum", "/src", "/dst"}, task.JobTypeCksum)
	if err := task.WriteMetaTo(meta, dir); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	cmd := newTaskCmd(t, dir)
	out, err := captureStdout(t, func() error {
		return runTaskShow(cmd, []string{taskID})
	})
	if err != nil {
		t.Fatalf("runTaskShow: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v\nout: %s", err, out)
	}
	if got["taskId"] != taskID {
		t.Errorf("taskId = %v, want %s", got["taskId"], taskID)
	}
	runs, ok := got["runs"].([]any)
	if !ok || len(runs) == 0 {
		t.Fatalf("expected runs array, got %v", got["runs"])
	}
	run0 := runs[0].(map[string]any)
	if run0["jobType"] != "cksum" {
		t.Errorf("first run jobType = %v, want cksum", run0["jobType"])
	}
}

func TestRunTaskDelete_NotFound(t *testing.T) {
	dir := t.TempDir()
	cmd := newTaskCmd(t, dir)

	err := runTaskDelete(cmd, []string{"task-nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent task, got nil")
	}
	if !strings.Contains(err.Error(), "cannot delete") {
		t.Errorf("error should start with 'cannot delete', got: %v", err)
	}
}

func TestRunTaskDelete_Success(t *testing.T) {
	dir := t.TempDir()
	taskID := "task-delete-cccc"
	// Use PID=1 (init) so CheckTaskNotRunning treats it as stale on most systems.
	// init is alive on Unix, but flock should succeed since no process holds it.
	meta := task.NewMeta(taskID, "/src", "/dst", []string{"copy"}, task.JobTypeCopy)
	meta.PID = -1 // unmistakably-dead PID — IsProcessAlive must return false
	if err := task.WriteMetaTo(meta, dir); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	taskDir := filepath.Join(dir, taskID)
	if _, err := os.Stat(taskDir); err != nil {
		t.Fatalf("task dir not created: %v", err)
	}

	cmd := newTaskCmd(t, dir)
	out, err := captureStdout(t, func() error {
		return runTaskDelete(cmd, []string{taskID})
	})
	if err != nil {
		t.Fatalf("runTaskDelete: %v", err)
	}
	if !strings.Contains(out, "\"deleted\"") {
		t.Errorf("expected deletion confirmation, got: %s", out)
	}
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Errorf("task dir should be gone, stat err: %v", err)
	}
}

func TestNewConfigCmd_Structure(t *testing.T) {
	cmd := newConfigCmd()
	if cmd.Use != "config" {
		t.Errorf("Use = %q, want config", cmd.Use)
	}
	subs := cmd.Commands()
	if len(subs) != 1 {
		t.Fatalf("expected 1 subcommand, got %d", len(subs))
	}
	if subs[0].Use != "show" {
		t.Errorf("subcommand Use = %q, want show", subs[0].Use)
	}
	if subs[0].Flags().Lookup("profile") == nil {
		t.Error("show command must have --profile flag")
	}
}

func TestResolveBoolFlag(t *testing.T) {
	// resolveBoolFlag mutates the package-level viper `v`. Reset it for each subtest.
	tests := []struct {
		name    string
		enable  bool
		disable bool
		want    bool
	}{
		{"both off → unchanged (defaults to false)", false, false, false},
		{"enable wins", true, false, true},
		{"disable wins over enable", true, true, false},
		{"only disable", false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v = viper.New()
			v.Set("Test", "sentinel-value-must-be-overwritten")

			cmd := &cobra.Command{Use: "x"}
			cmd.Flags().Bool("enable-Test", tt.enable, "")
			cmd.Flags().Bool("disable-Test", tt.disable, "")
			// cobra evaluates default values without setting them as Changed; the helper
			// reads the values via GetBool, which returns defaults when unchanged.

			resolveBoolFlag(cmd, "Test", "enable-Test", "disable-Test")

			// If both flags are at default-false, helper leaves v alone.
			// In that case the sentinel survives.
			if !tt.enable && !tt.disable {
				if v.GetString("Test") != "sentinel-value-must-be-overwritten" {
					t.Errorf("expected sentinel preserved, got %v", v.Get("Test"))
				}
				return
			}
			if v.GetBool("Test") != tt.want {
				t.Errorf("v[Test] = %v, want %v", v.GetBool("Test"), tt.want)
			}
		})
	}
}
