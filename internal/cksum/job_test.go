package cksum

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/pkg/impls/progress/pebble"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

func TestCksumJobOptions(t *testing.T) {
	j := NewCksumJob(nil, nil, nil,
		WithCksumParallelism(4),
		WithCksumTaskID("test-task"),
		WithCksumAlgo(model.CksumMD5),
		WithCksumChannelBuf(1000),
		WithCksumResume(true),
		WithCksumSkipByMtime(true),
		WithCksumIOSize(4096),
	)
	if j.parallelism != 4 {
		t.Errorf("parallelism = %d, want 4", j.parallelism)
	}
	if j.taskID != "test-task" {
		t.Errorf("taskID = %q, want test-task", j.taskID)
	}
	if j.cksumAlgo != model.CksumMD5 {
		t.Errorf("cksumAlgo = %v, want MD5", j.cksumAlgo)
	}
	if j.channelBuf != 1000 {
		t.Errorf("channelBuf = %d, want 1000", j.channelBuf)
	}
	if !j.resume {
		t.Error("resume = false, want true")
	}
	if !j.skipByMtime {
		t.Error("skipByMtime = false, want true")
	}
	if j.ioSize != 4096 {
		t.Errorf("ioSize = %d, want 4096", j.ioSize)
	}
}

func TestCksumJobDefaults(t *testing.T) {
	j := NewCksumJob(nil, nil, nil)
	if j.parallelism != 1 {
		t.Errorf("default parallelism = %d, want 1", j.parallelism)
	}
	if j.cksumAlgo != model.DefaultCksumAlgorithm {
		t.Errorf("default cksumAlgo = %v, want %v", j.cksumAlgo, model.DefaultCksumAlgorithm)
	}
	if j.channelBuf != 100000 {
		t.Errorf("default channelBuf = %d, want 100000", j.channelBuf)
	}
	if j.metrics == nil {
		t.Error("default metrics should not be nil")
	}
}

func TestCksumJobRunPass(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write src file1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "file1.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write dst file1: %v", err)
	}

	src, err := local.NewSource(srcDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	dst, err := local.NewSource(dstDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	storeDir := filepath.Join(t.TempDir(), "progress")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("mkdir storeDir: %v", err)
	}
	store := &pebble.Store{}
	if err := store.Open(storeDir); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	j := NewCksumJob(src, dst, store, WithCksumParallelism(2))
	exitCode, err := j.Run(context.Background())

	if exitCode != 0 {
		t.Errorf("exitCode = %d, want 0", exitCode)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCksumJobRunMismatch(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write src file1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "file1.txt"), []byte("world"), 0o644); err != nil {
		t.Fatalf("write dst file1: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	storeDir := filepath.Join(t.TempDir(), "progress")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("mkdir storeDir: %v", err)
	}
	store := &pebble.Store{}
	if err := store.Open(storeDir); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	j := NewCksumJob(src, dst, store, WithCksumParallelism(1))
	exitCode, err := j.Run(context.Background())

	if exitCode != 2 {
		t.Errorf("exitCode = %d, want 2", exitCode)
	}
	if err == nil {
		t.Error("expected error for mismatch, got nil")
	}
}

func TestCksumJobRunMissingDst(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write src file1: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	storeDir := filepath.Join(t.TempDir(), "progress")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("mkdir storeDir: %v", err)
	}
	store := &pebble.Store{}
	if err := store.Open(storeDir); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	j := NewCksumJob(src, dst, store, WithCksumParallelism(1))
	exitCode, err := j.Run(context.Background())

	if exitCode != 2 {
		t.Errorf("exitCode = %d, want 2", exitCode)
	}
	if err == nil {
		t.Error("expected error for missing dst file, got nil")
	}
}
